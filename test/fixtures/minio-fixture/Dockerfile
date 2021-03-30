FROM minio/minio:RELEASE.2021-03-01T04-20-55Z

ARG bucket
ARG accessKey
ARG secretKey

RUN mkdir -p /minio/data1/${bucket}
RUN mkdir -p /minio/data2/${bucket}
RUN mkdir -p /minio/data3/${bucket}
RUN mkdir -p /minio/data4/${bucket}
ENV MINIO_ACCESS_KEY=${accessKey}
ENV MINIO_SECRET_KEY=${secretKey}
