FROM golang@sha256:e30143be198ab04cf7ba25fba83ab3a692ca584c994aad0bf131fa0eb32dd8c1
COPY observer.c probe.c go-probe.go check.py /
RUN cc -std=c11 -O2 -Wall -Wextra -Werror -static /observer.c -o /exec-observer && \
    cc -std=c11 -O2 -Wall -Wextra -Werror -static -DDISABLE_CLONE_GUARD /observer.c -o /exec-observer-disabled && \
    cc -std=c11 -O2 -Wall -Wextra -Werror -static -DDISABLE_EXITKILL /observer.c -o /exec-observer-no-exitkill && \
    cc -std=c11 -O2 -Wall -Wextra -Werror -static -pthread /probe.c -o /exec-probe && \
    CGO_ENABLED=0 GOPROXY=off go build -o /go-probe /go-probe.go
ENTRYPOINT ["python3", "/check.py"]
