##趣活数据组内部git

#####框架结构

>quhuo_utils #基本公共类库/方法(与业务无关的)  
> >csv #处理csv相关操作的类或者方法放入此文件夹  
> > >csvhelper.py  #csv文件的处理  

> >excel #处理excel相关操作的类或者方法 
> > >excelhelper.py #excel相关操作的类或者方法  
 
> >datetime #时间相关的格式化  

> >url #地址处理相关  

> >os # os模块  

> >..... #其它通用的代码  

>vendor #第三方包放此文件夹  

>yaml #yml配置文件放此文件夹
> >...... #具体根据业务自行新建文件夹存放  

>data_common #业务相关公用的方法
> >...... #具体根据业务自行新建文件夹存放