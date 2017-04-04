#ifndef TPRINTF_H
#define TPRINTF_H

#define tprintf(args...) do { \
        struct timeval tv;     \
        gettimeofday(&tv, 0); \
        printf("%ld:\t", tv.tv_sec * 1000 + tv.tv_usec / 1000);\
        printf(args);   \
        } while (0);


#define tfprintf(args...) do { \
        struct timeval tv;     \
        gettimeofday(&tv, 0); \
        fprintf(stderr, "%ld:\t", tv.tv_sec * 1000 + tv.tv_usec / 1000);\
        fprintf(stderr, args);   \
        } while (0);
#endif
