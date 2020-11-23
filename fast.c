#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/timerfd.h>
#include <sys/types.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <netdb.h>
#include <time.h>
#include <netinet/in.h>
 
#include <curl/curl.h>

#define MSG_OUT stdout
#define DEFAULT_QUEUE_LENGTH 10000
#define mycase(code) \
        case code: s = __STRING(code)

#define MAX_CONNECTIONS 1024

/* Global information, common to all connections */
typedef struct _GlobalInfo
{
	int epfd;    /* epoll filedescriptor */
	int tfd;     /* timer filedescriptor */
	CURLM *multi;
	int still_running;
	pthread_mutex_t lock;
	int concurrent_connections;
	pthread_mutex_t parsed_lock;
	int parsed_sites;
	int transfers;
} GlobalInfo;

/* Information associated with a specific easy handle */
typedef struct _ConnInfo
{
	CURL *easy;
	char *url;
	GlobalInfo *global;
	char error[CURL_ERROR_SIZE];
	size_t size;
	char *data;
} ConnInfo;

/* Information associated with a specific socket */
typedef struct _SockInfo
{
	curl_socket_t sockfd;
	CURL *easy;
	int action;
	long timeout;
	GlobalInfo *global;
} SockInfo;

/* Die if we get a bad CURLMcode somewhere */ 
static void
mcode_or_die(const char *where, CURLMcode code)
{
	if (CURLM_OK != code) {
		const char *s;

		switch (code) {
			mycase(CURLM_BAD_HANDLE); break;
			mycase(CURLM_BAD_EASY_HANDLE); break;
			mycase(CURLM_OUT_OF_MEMORY); break;
			mycase(CURLM_INTERNAL_ERROR); break;
			mycase(CURLM_UNKNOWN_OPTION); break;
			mycase(CURLM_LAST); break;
			default: s = "CURLM_unknown"; break;
			mycase(CURLM_BAD_SOCKET);
			fprintf(MSG_OUT, "ERROR: %s returns %s\n", where, s);
			/* ignore this error */ 
			return;
		}

		fprintf(MSG_OUT, "ERROR: %s returns %s\n", where, s);
		exit(code);
	}
}

void
print_progress(GlobalInfo *g)
{
	printf("\rParsed sites: %d, %d parallel connections, %d still running, %d transfers\t", 
			g->parsed_sites, g->concurrent_connections, g->still_running, g->transfers);
	fflush(stdout);
}

void
transfers_inc(GlobalInfo *g)
{
	g->transfers++;

	print_progress(g);
}

void
transfers_dec(GlobalInfo *g)
{
	g->transfers--;

	print_progress(g);
}

void
concurrent_connections_inc(GlobalInfo *g)
{
	g->concurrent_connections++;

	print_progress(g);
}

void
concurrent_connections_dec(GlobalInfo *g)
{
	g->concurrent_connections--;

	print_progress(g);
}

static void timer_cb(GlobalInfo* g, int revents);
 
/* Update the timer after curl_multi library does it's thing. Curl will
 * inform us through this callback what it wants the new timeout to be,
 * after it does some work. */ 
static int
multi_timer_cb(CURLM *multi, long timeout_ms, GlobalInfo *g)
{
	struct itimerspec its;
 
	//fprintf(MSG_OUT, "multi_timer_cb: Setting timeout to %ld ms\n", timeout_ms);
 
	if (timeout_ms > 0) {
		its.it_interval.tv_sec = 1;
		its.it_interval.tv_nsec = 0;
		its.it_value.tv_sec = timeout_ms / 1000;
		its.it_value.tv_nsec = (timeout_ms % 1000) * 1000 * 1000;
	} else if(timeout_ms == 0) {
		/* libcurl wants us to timeout now, however setting both fields of
		 * new_value.it_value to zero disarms the timer. The closest we can
		 * do is to schedule the timer to fire in 1 ns. */ 
		its.it_interval.tv_sec = 1;
		its.it_interval.tv_nsec = 0;
		its.it_value.tv_sec = 0;
		its.it_value.tv_nsec = 1;
	} else {
		memset(&its, 0, sizeof(struct itimerspec));
	}
 
	timerfd_settime(g->tfd, /*flags=*/ 0, &its, NULL);

	return (0);
}
 
/* Check for completed transfers, and remove their easy handles */ 
static void
check_multi_info(GlobalInfo *g)
{
	char *eff_url;
	CURLMsg *msg;
	int msgs_left;
	ConnInfo *conn;
	CURL *easy;
	char *ct;
	double time;
	double dl;
	//CURLcode res;
 
	while ((msg = curl_multi_info_read(g->multi, &msgs_left))) {
		if (msg->msg == CURLMSG_DONE) {
			easy = msg->easy_handle;
			//res = msg->data.result;
			curl_easy_getinfo(easy, CURLINFO_PRIVATE, &conn);
			curl_easy_getinfo(easy, CURLINFO_EFFECTIVE_URL, &eff_url);
			curl_easy_getinfo(easy, CURLINFO_CONTENT_TYPE, &ct);
			curl_easy_getinfo(easy, CURLINFO_TOTAL_TIME, &time);
			curl_easy_getinfo(easy, CURLINFO_SIZE_DOWNLOAD, &dl);

			//fprintf(MSG_OUT, "DONE: %s => (%d) %s\n", eff_url, res, conn->error);

			curl_multi_remove_handle(g->multi, easy);
			//free(conn->url);
			free(conn->data);
			curl_easy_cleanup(easy);
			transfers_dec(g);
			free(conn);
		}
	}
}
 
/* Called by libevent when we get action on a multi socket filedescriptor*/ 
static void
event_cb(GlobalInfo *g, int fd, int revents)
{
	CURLMcode rc;
	struct itimerspec its;
 
	int action = ((revents & EPOLLIN) ? CURL_CSELECT_IN : 0) |
				 ((revents & EPOLLOUT) ? CURL_CSELECT_OUT : 0);
 
	rc = curl_multi_socket_action(g->multi, fd, action, &g->still_running);
	mcode_or_die("event_cb: curl_multi_socket_action", rc);
 
	check_multi_info(g);

	if (g->still_running <= 0) {
		//fprintf(MSG_OUT, "last transfer done, kill timeout\n");
		memset(&its, 0, sizeof(struct itimerspec));
		timerfd_settime(g->tfd, 0, &its, NULL);
	}
}
 
/* Called by main loop when our timeout expires */ 
static void
timer_cb(GlobalInfo* g, int revents)
{
	CURLMcode rc;
	uint64_t count = 0;
	ssize_t err = 0;
 
	err = read(g->tfd, &count, sizeof(uint64_t));
	if (err == -1) {
		/* Note that we may call the timer callback even if the timerfd isn't
		 * readable. It's possible that there are multiple events stored in the
		 * epoll buffer (i.e. the timer may have fired multiple times). The
		 * event count is cleared after the first call so future events in the
		 * epoll buffer will fail to read from the timer. */ 
		if (errno == EAGAIN) {
			//fprintf(MSG_OUT, "EAGAIN on tfd %d\n", g->tfd);
			return;
		}
	}

	if (err != sizeof(uint64_t)) {
		fprintf(stderr, "read(tfd) == %ld", err);
		perror("read(tfd)");
	}
 
	rc = curl_multi_socket_action(g->multi, CURL_SOCKET_TIMEOUT, 0, &g->still_running);
	mcode_or_die("timer_cb: curl_multi_socket_action", rc);
	check_multi_info(g);
}

/* Assign information to a SockInfo structure */ 
static void
setsock(SockInfo *f, curl_socket_t s, CURL *e, int act, GlobalInfo *g)
{
	struct epoll_event ev;
	int kind = ((act & CURL_POLL_IN) ? EPOLLIN : 0) |
			   ((act & CURL_POLL_OUT) ? EPOLLOUT : 0);
 
	if (f->sockfd) {
		concurrent_connections_dec(g);
		if (epoll_ctl(g->epfd, EPOLL_CTL_DEL, f->sockfd, NULL))
			fprintf(stderr, "EPOLL_CTL_DEL failed for fd: %d : %s\n",
			  f->sockfd, strerror(errno));
	}
 
	f->sockfd = s;
	f->action = act;
	f->easy = e;
 
	ev.events = kind;
	ev.data.fd = s;

	concurrent_connections_inc(g);
	if (epoll_ctl(g->epfd, EPOLL_CTL_ADD, s, &ev)) {
		fprintf(stderr, "EPOLL_CTL_ADD failed for fd: %d : %s\n",
		  s, strerror(errno));
	}
}
 
/* Initialize a new SockInfo structure */ 
static void
addsock(curl_socket_t s, CURL *easy, int action, GlobalInfo *g)
{
	SockInfo *fdp = (SockInfo *)calloc(sizeof(SockInfo), 1);
 
	fdp->global = g;
	setsock(fdp, s, easy, action, g);
	curl_multi_assign(g->multi, s, fdp);
}
 
static size_t
write_cb(void *contents, size_t size, size_t nmemb, void *p)
{
	ConnInfo *conn = (ConnInfo *)p;
	size_t realsize = size * nmemb;

	conn->data = realloc(conn->data, conn->size + realsize + 1);
	if (conn->data == NULL) {
		/* out of memory! */ 
		printf("not enough memory (realloc returned NULL)\n");
		return 0;
	}
 
	memcpy(&(conn->data[conn->size]), contents, realsize);
	conn->size += realsize;
	conn->data[conn->size] = 0;
 
	return realsize;
}

/* Create a new easy handle, and add it to the global curl_multi */ 
int
new_conn(char *url, GlobalInfo *g)
{
	ConnInfo *conn;
	CURLMcode rc;

	conn = (ConnInfo*)calloc(1, sizeof(ConnInfo));
	conn->error[0]='\0';
	conn->global = g;
 
	conn->easy = curl_easy_init();
	if (!conn->easy) {
		fprintf(MSG_OUT, "curl_easy_init() failed, exiting!\n");
		exit(2);
	}
	transfers_inc(g);

	conn->global = g;
	conn->url = url;
	curl_easy_setopt(conn->easy, CURLOPT_URL, conn->url);
	curl_easy_setopt(conn->easy, CURLOPT_WRITEFUNCTION, write_cb);
	curl_easy_setopt(conn->easy, CURLOPT_WRITEDATA, conn);
	curl_easy_setopt(conn->easy, CURLOPT_FOLLOWLOCATION, 1L);
	curl_easy_setopt(conn->easy, CURLOPT_ERRORBUFFER, conn->error);
	curl_easy_setopt(conn->easy, CURLOPT_PRIVATE, conn);
	curl_easy_setopt(conn->easy, CURLOPT_NOPROGRESS, 1L);
	curl_easy_setopt(conn->easy, CURLOPT_PROGRESSDATA, conn);
	curl_easy_setopt(conn->easy, CURLOPT_FOLLOWLOCATION, 1L);
	curl_easy_setopt(conn->easy, CURLOPT_LOW_SPEED_TIME, 3L);
	curl_easy_setopt(conn->easy, CURLOPT_LOW_SPEED_LIMIT, 100L);
	curl_easy_setopt(conn->easy, CURLOPT_CONNECTTIMEOUT, 10L);
	curl_easy_setopt(conn->easy, CURLOPT_CLOSESOCKETDATA, g);

	rc = curl_multi_add_handle(g->multi, conn->easy);
	mcode_or_die("new_conn: curl_multi_add_handle", rc);
 
	/* note that the add_handle() will set a time-out to trigger very soon so
     that the necessary socket_action() call will be called by this app */ 

	return (0);
}
 
/* Clean up the SockInfo structure */ 
static void
remsock(SockInfo *f, GlobalInfo* g)
{
	if (f) {
		if (f->sockfd) {
			concurrent_connections_dec(g);
			if (epoll_ctl(g->epfd, EPOLL_CTL_DEL, f->sockfd, NULL))
				fprintf(stderr, "EPOLL_CTL_DEL failed for fd: %d : %s\n",
				  f->sockfd, strerror(errno));
		}

		free(f);
	}
}
 
/* CURLMOPT_SOCKETFUNCTION */ 
static int
sock_cb(CURL *e, curl_socket_t s, int what, void *cbp, void *sockp)
{
	GlobalInfo *g = (GlobalInfo*) cbp;
	SockInfo *fdp = (SockInfo*) sockp;

	if (what == CURL_POLL_REMOVE) {
		remsock(fdp, g);
	} else {
		if (g->concurrent_connections < MAX_CONNECTIONS){
			if (!fdp) {
				addsock(s, e, what, g);
			} else {
				setsock(fdp, s, e, what, g);
			}
		}
	}

	return (0);
}

/* CURLMOPT_SOCKETFUNCTION */
static int
end_sock_cb(CURL *e, curl_socket_t s, int what, void *cbp, void *sockp)
{
        GlobalInfo *g = (GlobalInfo*) cbp;
        SockInfo *fdp = (SockInfo*) sockp;

        if (what == CURL_POLL_REMOVE) {
                remsock(fdp, g);
        }

        return (0);
}


int should_exit = 0;
 
void
signal_handler(int signo)
{
	should_exit = 1;
}

void *
crawler_init()
{
	GlobalInfo g;
	struct itimerspec its;
	struct epoll_event ev;
	struct epoll_event events[10000];


	memset(&g, 0, sizeof(GlobalInfo));

	g.epfd = epoll_create1(EPOLL_CLOEXEC);
	if (g.epfd == -1) {
		perror("epoll_create1 failed\n");
		exit(1);
	}
 
	g.tfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
	if (g.tfd == -1) {
		perror("timerfd_create failed\n");
		exit(1);
	}
 
	memset(&its, 0, sizeof(struct itimerspec));
	its.it_interval.tv_sec = 1;
	its.it_value.tv_sec = 1;
	timerfd_settime(g.tfd, 0, &its, NULL);
 
	ev.events = EPOLLIN;
	ev.data.fd = g.tfd;
	epoll_ctl(g.epfd, EPOLL_CTL_ADD, g.tfd, &ev);
 
	curl_global_init(CURL_GLOBAL_DEFAULT);
	g.multi = curl_multi_init();
 
	/* setup the generic multi interface options we want */ 
	curl_multi_setopt(g.multi, CURLMOPT_SOCKETFUNCTION, sock_cb);
	curl_multi_setopt(g.multi, CURLMOPT_SOCKETDATA, &g);
	curl_multi_setopt(g.multi, CURLMOPT_TIMERFUNCTION, multi_timer_cb);
	curl_multi_setopt(g.multi, CURLMOPT_TIMERDATA, &g);

	/* we don't call any curl_multi_socket*() function yet as we have no handles added! */ 

	//printf("Starting crawler...\n");

	while (!should_exit) {
		int idx;
		int err = epoll_wait(g.epfd, events, sizeof(events)/sizeof(struct epoll_event), 10000);


		new_conn("www.google.com", &g);
		new_conn("www.yahoo.com", &g);
		new_conn("www.bing.com", &g);

		if (err == -1) {
			if (errno == EINTR) {
				fprintf(MSG_OUT, "note: wait interrupted\n");
				continue;
			} else {
				perror("epoll_wait");
				exit(1);
			}
		}
 
		for (idx = 0; idx < err; ++idx) {
			if (events[idx].data.fd == g.tfd) {
				timer_cb(&g, events[idx].events);
			} else {
				event_cb(&g, events[idx].data.fd, events[idx].events);
			}
		}
	}
 
	fprintf(MSG_OUT, "Exiting normally.\n");
	fflush(MSG_OUT);
 
	curl_multi_setopt(g.multi, CURLMOPT_SOCKETFUNCTION, end_sock_cb);
	while (g.concurrent_connections > 0 || g.transfers > 0)
	{
		int idx;
                int err = epoll_wait(g.epfd, events, sizeof(events)/sizeof(struct epoll_event), 10000);

                if (err == -1) {
                        if (errno == EINTR) {
                                fprintf(MSG_OUT, "note: wait interrupted\n");
                                continue;
                        } else {
                                perror("epoll_wait");
                                exit(1);
                        }
                }

                for (idx = 0; idx < err; ++idx) {
                        if (events[idx].data.fd == g.tfd) {
                                timer_cb(&g, events[idx].events);
                        } else {
                                event_cb(&g, events[idx].data.fd, events[idx].events);
                        }
                }

	}

	fprintf(MSG_OUT, "Finished all in progress downloads.\n");
	fflush(MSG_OUT);

	curl_multi_cleanup(g.multi);
	curl_global_cleanup();

	return (NULL);
}

int
main(int argc, char **argv)
{
	int cleanup = 0, opt, ret;

	should_exit = 0;
	signal(SIGINT, signal_handler);
	signal(SIGKILL, signal_handler);

	crawler_init();


	printf("Exiting.\n");

	return (0);
}
