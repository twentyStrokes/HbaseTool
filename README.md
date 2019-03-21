程序执行命令
```
hadoop jar HbaseTool.jar <methodName> <tableName> <outputPath> <starttime> <endtime>
```
目前有三个方法
ExportPwmtqpag		绑卡信息 				PWMTQPAG表
ExportTxLog			快捷转账的交易流水数据	TX_LOG表
ExportUrmtpinf		客户基本信息数据			URMTPINF表
<methodName>也就是上面三个方法的方法名，选择想要执行的方法。

<tableName> 	Hbase中的表名

<outputPath> 	HDFS上的路径

<starttime>	开始时间戳（毫秒）

 <endtime>		结束时间戳（毫秒）

