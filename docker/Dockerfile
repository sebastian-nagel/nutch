# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM ubuntu:18.04
MAINTAINER Apache Nutch Committers <dev@nutch.apache.org>

WORKDIR /root/


# Install dependencies
RUN apt update
RUN apt install -y ant git openjdk-8-jdk-headless

# Set up JAVA_HOME
RUN echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> $HOME/.bashrc

# Checkout and build Nutch (testing IVY-1586)
RUN git clone https://github.com/sebastian-nagel/nutch.git nutch_source
RUN cd nutch_source && git checkout test-IVY-1586 && git pull origin test-IVY-1586 && git show
# test with intermediate containers to allow tracing the errors down:
# 1. Nutch core
RUN cd nutch_source && ant compile-core-test
RUN cd nutch_source && ant clean compile-core-test
# 2. Nutch plugins
RUN cd nutch_source && ant clean compile-plugins
RUN cd nutch_source && ant clean compile-plugins
# 3. build everythin
RUN cd nutch_source && ant clean runtime
RUN cd nutch_source && ant clean runtime
# 4. run all unit tests
RUN cd nutch_source && ant clean test

# Convenience symlink to Nutch runtime local
RUN ln -s nutch_source/runtime/local $HOME/nutch
