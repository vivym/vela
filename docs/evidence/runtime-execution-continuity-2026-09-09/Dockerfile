FROM golang@sha256:e30143be198ab04cf7ba25fba83ab3a692ca584c994aad0bf131fa0eb32dd8c1
COPY probe.c check.py /
RUN cc -std=c11 -O2 -Wall -Wextra -Werror -static -fno-pie -no-pie -DIMAGE_LABEL='"A"' /probe.c -o /probe-a && \
    cc -std=c11 -O2 -Wall -Wextra -Werror -static -fno-pie -no-pie -DIMAGE_LABEL='"B"' /probe.c -o /probe-b
ENTRYPOINT ["python3", "/check.py"]
