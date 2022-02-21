FROM tiger:test

RUN mkdir -p /workspace/helloTigerGraph
COPY ./ /workspace/helloTigerGraph
WORKDIR /workspace/helloTigerGraph
ENV TZ="Asia/Shanghai"

