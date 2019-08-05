#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import json 
import numpy as np
import re


# In[10]:


'''
    通过path值来获得
'''
def gettype(series):
    path=series["PAGEPATH"]
    if path.find('depart/')!=-1:
        loc=path.find('depart/')
        #print(loc,len(path))
        if (len(path)>=loc+12):
            return [2,re.findall(r'\d+',path[loc+7:loc+11])[0]]
        else:
            if(len(path)==loc+7):
                return [0,0]
            else:
                return [2,re.findall(r'\d+',path[loc+7:len(path)-1])[0]]
    if path.find('article/')!=-1:
        loc=path.find('article/')
        #print(loc,len(path))
        if len(path)>=loc+41:
            return [1,path[loc+8:loc+40]]
        else:
            if(len(path)==loc+8):
                return [0,0]
            else:
                return [1,path[loc+8:len(path)-1]]
    return [0,0]


# In[80]:


class analytics:
   
    '''
    原始数据是否处理都只需读取一次
    但考虑到数据后期需要日期来作为统计标准之一
    所以，还是需要向原始数据中添加日期，将列名设置为“DATE”
    '''    
    def __get_GA(self,path,day_count):
        
        try:
            df=pd.read_csv(path,encoding="gbk",header=0)
        except:
            df=pd.read_csv(path,encoding="gbk",header=5,skipfooter=day_count+4)
        else:
            return df
        finally:
            return df      
        
    '''
    向读取到的GA数据中
    通过path添加网页类型及其对应ID值
    调用gettype函数来返回series获得
    '''
    def addtype(self):
        s=self.df_ga.apply(gettype,axis=1,result_type='expand')
        s.columns=['网页类型','ID']
        #print(s)
        self.df_ga = pd.concat([self.df_ga, s], axis=1)
    '''
    关联(后来没在用了)
    '''    
    def get_art_sta(self):
        #将各文章与文章网页GA统计信息通过ID联系起来 
        art_ga=self.df_article.set_index("article_id").join(self.df_ga.set_index("ID")).fillna(0)
        return art_ga
    
    '''
    通过两个ID关联两表，返回关联好的表
    '''    
    def __join_df(self,df1,ID1,df2,ID2):
        #将各文章与文章网页GA统计信息通过ID联系起来 
        df_join=df1.set_index(ID1).join(df2.set_index(ID2))
        return df_join.fillna(0)
    '''
    获得支部简要信息统计表
    '''
    def __get_sim_dep(self):
        #----------先获得支部编号、支部名称、支部类型、文章数量----------------------
        #--------------滤出支部编号、支部名称、支部类型和 文章编号
        #-------------将支部编号、名称、类型groupby并count()
        #---文章编号列即为文章篇数
        df_sim_dep=self.df_article.filter(
            items=['department_id','department_name','department_type','article_id','PV']).\
            groupby(by=['department_id','department_name','department_type'],as_index=False).count()
        df_sim_dep.rename(columns={'article_id' : 'article_count'},inplace=True)
                
        return df_sim_dep
    
    '''
    获得按月PV值
    其中，df 应 group by ID&DATE 并 sum()
    '''
    def __get_mon_ga(self,df,itme):

        #-------------自动生成日期-----------------------
      
        month=[201901]
        for i in range(6):
            month.append(month[0]+1+i)
        #-------------遍历日期，获得各月PV值-------------    
        df_by_mon_pv=pd.DataFrame([])
        for mon in month:
            col=str(mon)+itme
            df_by_mon_pv=pd.concat(            [df_by_mon_pv,             df.filter(items=[itme]).query('DATE==@mon').rename(columns={itme:col}).                 reset_index(level="DATE").drop(columns=["DATE"])])
        
        df_by_mon_pv=df_by_mon_pv.groupby(by=['ID']).sum()
        df_by_mon_pv["sum_PV"]=df_by_mon_pv.sum(axis=1)
        
        #-------------由于ID值此时为object类型，将其转化为数值型
        return df_by_mon_pv.reset_index(level='ID').                apply(pd.to_numeric, errors='ignore')        
            
    '''
    初始化以更好地读取数据
    '''
    def __init__(self,ga_csv_path,json_path,day_count):
        self.df_ga=self.__get_GA(ga_csv_path,day_count)
        self.df_article=pd.read_json("article.json",encoding='utf-8',orient='records')
        
        self.dic_name={"department_id":"支部编号",                       "department_name":"支部名称",                       "department_type":"支部类型",                       "title":"文章名称",                       "DATE":"月份",                       "article_count":"文章数目",                       "article_id":"文章编号",
                       "sum_PV":"PV值合计",\
                       "ind_sum_PV":"非文章页PV值合计",\
                       "art_sum_PV":"文章页PV值合计"
                       }
        
    def rename_save(self,df,index_name,sheet_name):
        df.rename(columns=self.dic_name,index={0:index_name}).to_excel("DJ_STA.xlsx",sheet_name=sheet_name)        
        
    def getsta(self):
        
        analytics.addtype(self)
        #print(self.df_ga)
       
        #------获得各支部编号、名称、类型------
        self.df_sim_dep=self.__get_sim_dep()
        
         #------获得各文章及其GA统计信息
        self.df_sim_art=self.df_article.filter(items=['article_id','title','department_id','department_name','department_type'])
        self.df_art_ga=self.__join_df(self.df_article,"article_id",self.df_ga,"ID")
        
        #------获得各支部非文章页各月统计------
        #将GA统计中的非文章页提取出来并按ID和DATE统计求和
        #得到各支部非文章页对应统计值
        self.df_ind=self.df_ga.query('网页类型==2').groupby(by=['ID','DATE']).sum()
        self.df_dep_ind_mon=self.__get_mon_ga(self.df_ind,"PV")
        
        #------获得各支部各月统计中非文章页PV合计------
        self.df_dep_sta=self.__join_df(                self.df_sim_dep,                "department_id",                self.df_dep_ind_mon.filter(items=["ID","sum_PV"]).rename(columns={"sum_PV":"ind_sum_PV"}),                "ID")
        #----------------------------------------------
        
        self.df_dep_ind_mon=self.__join_df(self.df_sim_dep.drop(columns=["article_count"]),"department_id",self.df_dep_ind_mon,"ID")
        
       
        #------获得文章页各月统计--------------
        self.df_art=self.df_ga.query('网页类型==1').groupby(by=['ID','DATE']).sum()
        self.df_art_mon=self.__get_mon_ga(self.df_art,"PV")
        self.df_art_mon=self.__join_df(self.df_sim_art,'article_id',self.df_art_mon,"ID")
            
        #------获得各支部文章页各月统计-pri-------       
        self.df_dep_art_mon=self.__get_mon_ga(                                          self.df_art_ga.reset_index().                                          rename(columns={"index":"article_id","department_id":"ID"}).                                          groupby(by=['ID','DATE']).sum(),"PV")
       
        
         #------获得各支部各月统计中文章页PV合计------
        self.df_dep_sta=self.__join_df(                self.df_dep_sta.reset_index(),                "index",                self.df_dep_art_mon.filter(items=["ID","sum_PV"]).rename(columns={"sum_PV":"art_sum_PV"}),                "ID")
        #-----------------------------
        self.df_dep_art_mon=self.__join_df(self.df_sim_dep,"department_id",self.df_dep_art_mon,"ID")
        
        self.df_dep_sta=self.df_dep_sta.eval('sum_PV=ind_sum_PV+art_sum_PV')
        
    '''
    保存各个df
    '''    
    def save(self):
        with pd.ExcelWriter('DJ_STA.xlsx') as writer: 
            self.df_dep_sta.rename(columns=self.dic_name,index={0:"编号"}).to_excel(writer, sheet_name="支部PV值统计")
            self.df_dep_ind_mon.rename(columns=self.dic_name,index={0:"支部编号"}).to_excel(writer, sheet_name="各支部非文章页各月PV值统计")
            self.df_art_mon.rename(columns=self.dic_name,index={0:"文章编号"}).to_excel(writer, sheet_name="各文章各月PV值统计")
            self.df_dep_art_mon.rename(columns=self.dic_name,index={0:"支部编号"}).to_excel(writer, sheet_name="各支部文章页各月PV值统计")
      
       
        
        


# In[81]:


a=analytics("analytics_DJ.csv","article.json",0)
a.getsta()
a.save()


# In[ ]:




