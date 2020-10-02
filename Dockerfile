# BUILD starkdg/auscout:${VERSION}-${ARCH}-${OSNICK}

ARG REDIS_VER=6.0.7

# bionic|stretch|buster|etc
ARG OSNICK=buster

# debian:buster-slim|debian:stretch-slim|ubuntu:bionic
ARG OS=debian:buster-slim

# x64|arm64v8|arm32v7|etc
ARG ARCH=x64

#---------------------------------------------------------------------

FROM redisfab/redis:${REDIS_VER}-${ARCH}-${OSNICK} AS redis

# Build based on ${OS} (i.e. 'builder'), redis files are copies from 'redis'
FROM ${OS} AS builder

# Re-introduce arguments to this image
ARG OSNICK
ARG OS
ARG ARCH
ARG REDIS_VER

RUN echo "Building for $OSNICK ($OS) FOR $ARCH"

WORKDIR /build
COPY --from=redis /usr/local/ /usr/local/
ADD CMakeLists.txt module.cpp redismodule.h  /build/

RUN set -ex;\
	apt-get -q update ;\
	apt-get install -y --no-install-recommends ca-certificates wget;\
	apt-get install -y --no-install-recommends build-essential cmake

RUN set -ex;\
 	cmake -DCMAKE_BUILD_TYPE=Release . ;\
	make

#--------------------------------------------------------------------

FROM redisfab/redis:${REDIS_VER}-${ARCH}-${OSNICK} 

ARG OSNICK
ARG OS
ARG ARCH
ARG REDIS_VER

WORKDIR /data

ENV LIBDIR /usr/lib/redis/modules

RUN mkdir -p $LIBDIR

COPY --from=builder /build/auscout.so "$LIBDIR"

EXPOSE 6379

CMD ["redis-server", "--loadmodule", "/usr/lib/redis/modules/auscout.so"]


