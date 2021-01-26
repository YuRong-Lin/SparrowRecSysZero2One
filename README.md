# SparrowRecSysZero2One
本项目参考[SparrowRecSys](https://github.com/wzhe06/SparrowRecSys.git)实现，从0到1实践该深度学习推荐系统。

## 环境搭建

### scala环境
#### 1、mac安装

brew install scala@2.11

    scala@2.11 is keg-only, which means it was not symlinked into /usr/local,
    because this is an alternate version of another formula.
    
    If you need to have scala@2.11 first in your PATH run:
    echo 'export PATH="/usr/local/opt/scala@2.11/bin:$PATH"' >> ~/.zshrc
    echo 'export PATH="/usr/local/opt/scala@2.11/bin:$PATH"' >> ~/.zshrc

问题：安装包下多了一个idea目录（软连接），导致项目编译时报存在多个scala-library.jar包。

解决：删掉idea目录即可。

#### 2、windows安装

下载链接：https://www.scala-lang.org/download/2.11.12.html

下载该包： scala-2.11.12.msi	Windows (msi installer)	109.82M

配置环境变量：

* 新建系统变量SCALA_HOME：　D:\Program Files (x86)\scala
* PATH：　D:\Program Files (x86)\scala\bin

验证：   
scala -version  

### hadoop集群
hadoop集群采用apache hadoop 2.7.2版本

注：hadoop有多种运行模式：本地运行模式、伪分布运行环境、完全分布式运行环境。本项目搭建完全分布式运行环境。

#### 步骤
1）准备3台客户机（关闭防火墙、静态ip、主机名称）  
2）安装JDK  
3）配置环境变量  
4）安装Hadoop  
5）配置环境变量  
6）配置集群  
7）单点启动  
8）配置ssh  
9）群起并测试集群  

#### 安装
1. Hadoop下载地址：  
https://archive.apache.org/dist/hadoop/common/hadoop-2.7.2/  
将安装包下载到 /opt/software  

2. 安装路径：/opt/module  
tar -zxvf hadoop-2.7.2.tar.gz -C /opt/module/

3. 将Hadoop添加到环境变量和验证  
vim /etc/profile  


    export HADOOP_HOME=/opt/module/hadoop-2.7.2  
    export PATH=$PATH:$HADOOP_HOME/bin  
    export PATH=$PATH:$HADOOP_HOME/sbin  
    
    source /etc/profile
    
    hadoop version


4. 重要目录：  
* bin：存放对Hadoop相关服务（HDFS,YARN）进行操作的脚本  
* etc：Hadoop的配置文件目录，存放Hadoop的配置文件  
* lib：存放Hadoop的本地库（对数据进行压缩解压缩功能）  
* sbin：存放启动或停止Hadoop相关服务的脚本  
* share：存放Hadoop的依赖jar包、文档、和官方案例  

#### 集群配置
##### 修改hostname
    
    
    每台机分别执行：hostnamectl set-hostname rec-hadoop01/rec-hadoop02/rec-hadoop03
    
##### 集群部署规划

| | rec-hadoop01 | rec-hadoop02 | rec-hadoop03 |  
---------- | ---------- | --------| -------- |
HDFS | NameNode/DataNode | DataNode | SecondaryNameNode/DataNode | 
YARN | NodeManager  | ResourceManager/NodeManager | NodeManager |


##### 集群配置

###### 配置文件说明  
Hadoop配置文件分两类：默认配置文件和自定义配置文件，只有用户想修改某一默认配置值时，才需要修改自定义配置文件，更改相应属性值。  
    
###### 默认配置文件：
    
要获取的默认文件 | 文件存放在Hadoop的jar包中的位置 |
------- | -------- |
[core-default.xml] | hadoop-common-2.7.2.jar/ core-default.xml |
[hdfs-default.xml] | hadoop-hdfs-2.7.2.jar/ hdfs-default.xml |
[yarn-default.xml] | hadoop-yarn-common-2.7.2.jar/ yarn-default.xml |
[mapred-default.xml] | hadoop-mapreduce-client-core-2.7.2.jar/ mapred-default.xml |  

###### 自定义配置文件：  
core-site.xml、hdfs-site.xml、yarn-site.xml、mapred-site.xml四个配置文件存放在$HADOOP_HOME/etc/hadoop这个路径上，用户可以根据项目需求重新进行修改配置。

###### 核心配置文件
1) 配置core-site.xml  

       <!-- 指定HDFS中NameNode的地址 -->
       <property>
	       <name>fs.defaultFS</name>
	       <value>hdfs://rec-hadoop01:9000</value>
       </property>

	<!-- 指定Hadoop运行时产生文件的存储目录 -->
	
       <property>
	       <name>hadoop.tmp.dir</name>
	       <value>/opt/module/hadoop-2.7.2/tmp</value>
       </property>
    
    
###### HDFS配置文件
 1) hadoop-env.sh
 
 
    	export JAVA_HOME=/opt/module/jdk1.8.0_181
        
 2) hdfs-site.xml
 
 
        <property>
	        <name>dfs.replication</name>
	        <value>3</value>
    	</property>
    
        <!-- 指定Hadoop辅助名称节点主机配置 -->
	
        <property>
	        <name>dfs.namenode.secondary.http-address</name>
	        <value>rec-hadoop03:50090</value>
        </property>
    
    
###### YARN配置文件
1) yarn-env.sh


        export JAVA_HOME=/opt/module/jdk1.8.0_181
        
2) yarn-site.xml


        <!-- Reducer获取数据的方式 -->
        <property>
	        <name>yarn.nodemanager.aux-services</name>
	        <value>mapreduce_shuffle</value>
        </property>
    
        <!-- 指定YARN的ResourceManager的地址 -->
        <property>
	        <name>yarn.resourcemanager.hostname</name>
	        <value>rec-hadoop02</value>
        </property>      
    
        <!-- 日志聚集功能使能 -->
        <property>
	        <name>yarn.log-aggregation-enable</name>
	        <value>true</value>
        </property>
    
        <!-- 日志保留时间设置3天 -->
        <property>
	        <name>yarn.log-aggregation.retain-seconds</name>
	        <value>259200</value>
        </property>
    
        <!-- 日志链接跳转地址 -->
        <property>
	        <name>yarn.log.server.url</name>
	        <value>http://rec-hadoop03:19888/jobhistory/logs</value>
        </property>      
    
    
###### MapReduce配置文件  
1) mapred-env.sh


        export JAVA_HOME=/opt/module/jdk1.8.0_181
     
2) mapred-site.xml  

  
        cp mapred-site.xml.template mapred-site.xml
        vim mapred-site.xml
    
        <!-- 指定MR运行在Yarn上 -->
        <property>
	        <name>mapreduce.framework.name</name>
	        <value>yarn</value>
        </property>
    
        <!-- 历史服务器端地址 -->
        <property>
	        <name>mapreduce.jobhistory.address</name>
	        <value>rec-hadoop03:10020</value>
        </property>
        <!-- 历史服务器web端地址 -->
        <property>
	        <name>mapreduce.jobhistory.webapp.address</name>
	        <value>rec-hadoop03:19888</value>
        </property>


#### 集群单点启动(一般不采用这种方式，效率太低，而是采用群起集群)

##### 如果集群是第一次启动，需要格式化NameNode


    hadoop namenode -format
    
##### 在rec-hadoop01上启动NameNode


    hadoop-daemon.sh start namenode
    jps
    
##### 在rec-hadoop01、rec-hadoop02以及rec-hadoop03上分别启动DataNode


    hadoop-daemon.sh start datanode
    jps
    
    
#### SSH 无密登录配置

##### 免密登录原理，如图:


##### 生成公钥和私钥：


    cd ~/.ssh
    ssh-keygen -t rsa    
    然后敲（三个回车），就会生成两个文件id_rsa（私钥）、id_rsa.pub（公钥）
        
##### 将公钥拷贝到要免密登录的目标机器上   
 
 
    ssh-copy-id rec-hadoop01   
    ssh-copy-id rec-hadoop02   
    ssh-copy-id rec-hadoop03  
        
**注意：以上步骤需要在3台主机上分别执行**  

##### .ssh文件夹下（~/.ssh）的文件功能解释

known_hosts | 记录ssh访问过计算机的公钥(public key) |  
--------- | --------|  
id_rsa | 生成的私钥 |  
id_rsa.pub | 生成的公钥 |  
authorized_keys | 存放授权过得无密登录服务器公钥 |  


#### 群起集群

##### 配置slaves


    cd etc/hadoop/slaves
    vim slaves
    
    增加：
    rec-hadoop01
    rec-hadoop02
    rec-hadoop03
    
**如果集群是第一次启动，需要格式化NameNode（注意格式化之前，一定要先停止上次启动的所有namenode和datanode进程，然后再删除data和log数据）**

    
    cd /opt/module/hadoop-2.7.2
    bin/hdfs namenode -format
    
    启动HDFS：
    sbin/start-dfs.sh
    jps
    
    启动YARN：
    sbin/start-yarn.sh
    
**注意：NameNode和ResourceManger如果不是同一台机器，不能在NameNode上启动 YARN，应该在ResouceManager所在的机器上启动YARN。 **


#### 集群基本测试

    
    查看目录：
    hadoop fs -ls /
    或： hdfs dfs -ls /

    创建目录：
    hadoop fs -mkdir -p /user/root/input
    或：hdfs dfs -mkdir -p /user/root/input
    
    上传文件：
    hadoop fs -put wcinput/wc.input /user/root/input
    或：hdfs dfs -put wcinput/wc.input /user/root/input

    文件所在路径：
    /opt/module/hadoop-2.7.2/tmp/dfs/data/current
    
    下载文件：
    hadoop fs -get /user/root/input/wc.input ./
    
    删除文件：
    hadoop fs -rm -r /user/root/output
    
    
#### 集群启动/停止方式总结  
   * 各个服务组件逐一启动/停止  
        * 分别启动/停止HDFS组件  
   		hadoop-daemon.sh  start / stop  namenode / datanode / secondarynamenode  
        * 启动/停止YARN  
   		yarn-daemon.sh  start / stop  resourcemanager / nodemanager  
   		* 启动历史服务器
        sbin/mr-jobhistory-daemon.sh start historyserver  
        查看JobHistory：http://rec-hadoop03:19888/jobhistory
   		
   * 各个模块分开启动/停止（配置ssh是前提）(常用)  
   	    * 整体启动/停止HDFS  
   		start-dfs.sh   /  stop-dfs.sh  
        * 整体启动/停止YARN  
   		start-yarn.sh  /  stop-yarn.sh  

#### 执行WordCount实例


    hadoop fs -mkidr -p /test/input
    hadoop fs -put wcinput/wc.input /test/input
    hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.2.jar wordcount /test/input /test/wcoutput
    hadoop fs -cat /test/wcoutput/part-r-00000
        doop    1
        hadoop  1
        hello   1
        mapreduce       1
        world   1
        yarn    1

#### 界面查看

    
    hdfs：
    http://rec-hadoop01:50070/dfshealth.html#tab-overview
    
    yarn：
    http://rec-hadoop02:8088/cluster/cluster
    http://rec-hadoop03:19888/jobhistory


### spark运行环境
spark采用2.4.3版本


#### Maven方式

    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-core_${scala.version}</artifactId>
        <version>${spark.version}</version>
    </dependency>

    <!-- https://mvnrepository.com/artifact/org.apache.spark/spark-sql -->
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-sql_${scala.version}</artifactId>
        <version>${spark.version}</version>
    </dependency>

    <!-- https://mvnrepository.com/artifact/org.apache.spark/spark-mllib -->
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-mllib_${scala.version}</artifactId>
        <version>${spark.version}</version>
    </dependency>
    
采用该方式，在windows环境下启动spark，会报错：Could not locate executable null\bin\winutils.exe in the Hadoop binaries

解决：需要安装hadoop在windows下的支持插件：

	下载资源：
	1）http://archive.apache.org/dist/hadoop/core/  找对应版本
	2）https://github.com/cdarlint/winutils
	
在Path下配置好对应的bin路径即可。（如果未生效，可尝试重启电脑）

#### Yarn模式

##### 解压缩文件  
  tar -zxvf spark-2.4.3-bin-hadoop2.7.tgz -C /opt/module
    
##### 修改配置文件  

1）修改 hadoop 配置文件/opt/module/hadoop/etc/hadoop/yarn-site.xml
  
  
    <!--是否启动一个线程检查每个任务正使用的物理内存量，如果任务超出分配值，则直接将其杀掉，默认是 true -->
    <property>
        <name>yarn.nodemanager.pmem-check-enabled</name>
        <value>false</value>
    </property>
    
    <!--是否启动一个线程检查每个任务正使用的虚拟内存量，如果任务超出分配值，则直接将其杀掉，默认是 true -->
    <property>
        <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
    </property>
    
2）修改 conf/spark-env.sh，添加 JAVA_HOME 和 YARN_CONF_DIR 配置
  
  
    mv spark-env.sh.template spark-env.sh
    ...
    export JAVA_HOME=/opt/module/jdk1.8.0_181
    YARN_CONF_DIR=/opt/module/hadoop-2.7.2/etc/hadoop
    
    
##### 启动 HDFS 以及 YARN 集群

##### 提交应用
    bin/spark-submit --class org.apache.spark.examples.SparkPi --master yarn --deploy-mode cluster ./examples/jars/spark-examples_2.11-2.4.3.jar 10
    
    bin/spark-submit --class com.sparrowrecsys.offline.spark.embedding.Embedding --master yarn --deploy-mode cluster ./examples/jars/SparrowRecSysZero2One.jar file:///opt/module/spark-2.4.3/resources/ratings.csv /opt/module/spark-2.4.3/resources/item2vecEmb.csv 10
    

##### 配置历史服务器

1) 修改 spark-defaults.conf.template 文件名为 spark-defaults.conf  


    mv spark-defaults.conf.template spark-defaults.conf

2) 修改 spark-default.conf 文件，配置日志存储路径   


    spark.eventLog.enabled true  
    spark.eventLog.dir hdfs://rec-hadoop01:9000/directory  

注意：需要启动 hadoop 集群，HDFS 上的目录需要提前存在。  

    sbin/start-dfs.sh  
    hadoop fs -mkdir /directory  

3) 修改 spark-env.sh 文件, 添加日志配置


    <!-- 配置spark历史（需先在hdfs上创建/directory目录） -->
    export SPARK_HISTORY_OPTS="
    -Dspark.history.ui.port=18080
    -Dspark.history.fs.logDirectory=hdfs://rec-hadoop01:9000/directory
    -Dspark.history.retainedApplications=30"
    
    参数 1 含义：WEB UI 访问的端口号为 18080
    参数 2 含义：指定历史服务器日志存储路径
    参数 3 含义：指定保存 Application 历史记录的个数，如果超过这个值，旧的应用程序信息将被删除，这个是内存中的应用数，而不是页面上显示的应用数。

4) 修改 spark-defaults.conf  


    spark.yarn.historyServer.address=rec-hadoop01:18080  
    spark.history.ui.port=18080  

5) 启动历史服务  


    sbin/start-history-server.sh


### conda
https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html

    下载安装文件：Miniconda3-py37_4.9.2-Linux-x86_64.sh

    执行：
    bash Miniconda3-py37_4.9.2-Linux-x86_64.sh
    
    
### gcc 环境
dnf group install "Development Tools"


### 通过conda安装tf
    conda create -n sparrowRec python=3.7
    conda activate sparrowRec
    pip install --upgrade tensorflow -i http://pypi.douban.com/simple/ --trusted-host pypi.douban.com 


### jupyter notebook
    pip install jupyter notebook
  
#### 远程访问配置
##### 生成默认配置文件
    jupyter notebook --generate-config
    将会在用户主目录下生成.jupyter文件夹，其中jupyter_notebook_config.py就是刚刚生成的配置文件

##### 生成秘钥
    输入 ipython，进入ipyhon命令行
    输入
    In [1]: from notebook.auth import passwd

    In [2]: passwd()
    这里要求你输入以后登录使用的密码，然后生成一个秘钥，记得保存好秘钥，以免丢失。

    Enter password: 
    Verify password: 
    Out[2]: 'argon2:$argon2id$v=19$m=10240,t=10,p=8$zQmpCHcCTHa625NSKha7Qw$ncLzWZudQNa0/I07PwcskQ'
    
##### 修改配置文件
    修改用户主目录下~/.jupyter/jupyter_notebook_config.py文件  
    取消c.NotebookApp.password = ''"注释，并将生成的秘钥复制进去  

    c.NotebookApp.password = 'argon2:$argon2id$v=19$m=10240,t=10,p=8$zQmpCHcCTHa625NSKha7Qw$ncLzWZudQNa0/I07PwcskQ'
   
    取消下面几项注释，并注释修改ip、端口、不自动打开浏览器  
    c.NotebookApp.ip='*' #×允许任何ip访问  
    c.NotebookApp.open_browser = False  
    c.NotebookApp.port =8888 #可自行指定一个端口, 访问时使用该端口  
    c.NotebookApp.allow_remote_access = True
    c.NotebookApp.allow_root = True
    
    
#### 启动并访问http://rec-hadoop03:8888/
    jupyter notebook
    
    [W 19:09:17.996 NotebookApp] WARNING: The notebook server is listening on all IP addresses and not using encryption. This is not recommended.
    [I 19:09:17.999 NotebookApp] 启动notebooks 在本地路径: /root
    [I 19:09:18.000 NotebookApp] Jupyter Notebook 6.2.0 is running at:
    [I 19:09:18.000 NotebookApp] http://rec-hadoop03:8888/


### Docker
#### 安装yum-utils和添加docker下载源
    sudo yum install -y yum-utils
    
    sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
    
    官网可能比较慢，可采用阿里云加速：
    sudo yum-config-manager --add-repo https://mirrors.aliyun.com/docker-ce/linux/centos/docker-ce.repo

#### 安装与验证
    sudo yum install docker-ce docker-ce-cli containerd.io
    
    docker version
    
#### 启动docker
    sudo systemctl start docker

#### 设置开机自启
    sudo systemctl enable docker

#### 配置阿里云镜像下载加速
    登录阿里云，搜索“容器镜像服务”，找到镜像加速器的对应系统版本，按说明操作即可。
    
    如centos：
    sudo mkdir -p /etc/docker
    sudo tee /etc/docker/daemon.json <<-'EOF'
    {
      "registry-mirrors": ["https://hnah5dzx.mirror.aliyuncs.com"]
    }
    EOF
    sudo systemctl daemon-reload
    sudo systemctl restart docker
    
#### 修改默认网段（默认网段跟内网可能冲突）
    vim /etc/docker/daemon.json
    添加：
    "bip":"172.200.0.1/24"
    
    注：如果多台机器上都装了docker，这个网段需不一样：如："bip":"172.201.0.1/24"、"bip":"172.202.0.1/24"
    
    重启：
    systemctl restart docker
