import shutil
import os
import dbf
import re
import pythondemo
import time
# import tools
_status = {5:'任务开始',10:'参数校验成功',
         15:'CPP加载参数成功',20:'CPP解析参数成功',25:'CPP读取源文件dbf成功',
         30:'读取源文件dbf成功',40:'CPP获取过滤数据成功',60:'写本地dbf成功',
         90:'CPP写目的dbf成功',100:'拷贝到目的地成功,任务结束'}


class Task():
    def __init__(self, log, config, data, msg_queue=None, msg_dict=None):
        self.log = log
        self.data = data
        self.config = config
        self.msg_queue = msg_queue
        self.msg_dict = msg_dict
        self.id = data['id']
        self.fileid = self.data['attrib'].get('FileID','')
        self.filename = self.data['attrib'].get('Description','')
        self.filefrom = self.data['source'].get('FileName','')
        self.fileto = self.data['destination']
        self.filter = self.data['filter']
        self.filterflag = self.data['filterflag']
        targetfile = self.data['targetfile']
        self.iftxt = ('.txt' in self.filefrom.lower() or '.tsv' in self.filefrom.lower())
        self.dbfs = []
        self.targetrecords=[]

    def __del__(self):
        if not self.iftxt:
            for dbf in self.dbfs:
                dbf.close()

    def check_data(self):
        comp = {'COMP_EQUAL':lambda a,b : a==b,
                'COMP_NOTEQUAL':lambda a,b : a!=b,
                'COMP_LESS':lambda a,b : a<b,
                'COMP_NOTLESS':lambda a,b : a>=b,
                'COMP_GREAT':lambda a,b : a>b,
                'COMP_NOTGREAT':lambda a,b : a<=b,
                'COMP_INFILE':lambda a,b : a in b}
        for filter in self.filter:
            if filter.get('LinkType','').upper() not in ['AND','OR']:
                self.log.error('[%s] %s LinkType err:%s'%(self.id,self.fileid,filter['LinkType']))
                return False
            if filter.get('CompType','') not in comp:
                self.log.error('[%s] %s ComType err:%s'%(self.id,self.fileid,filter['CompType']))
                return False

            if '.txt' in self.filefrom or '.tsv' in self.filefrom:
                if not re.match("\d+$",filter.get('FieldID','')) and True or False:
                    self.log.debug(re.match(r"d+$",filter.get('FieldID','')))
                    self.log.error('[%s] %s txt FieldId[%s]  not integer : %s'%(self.id,self.fileid,filter.get('FieldID',''), self.filefrom))
                    return False
        if not os.path.exists(self.filefrom):
        # if not self.check_connect(self.filefrom):
            self.log.error('[%s] %s cannot get source dbf: %s'%(self.id,self.fileid,self.filefrom))
            return False
        return True

    def check_connect(self, path, time_out=3):
        if path[1] == ':':
            return os.path.exists(path)
        host = path[:path.find('\\',3)+1]+'ipc$'
        # result = tools.command_run('net use %s >nul 2>nul'%host, time_out)
        # return result == 0

    def get_comp_result(self,record):
        comp = {'COMP_EQUAL':lambda a,b : a==b,
                'COMP_NOTEQUAL':lambda a,b : a!=b,
                'COMP_LESS':lambda a,b : a<b,
                'COMP_NOTLESS':lambda a,b : a>=b,
                'COMP_GREAT':lambda a,b : a>b,
                'COMP_NOTGREAT':lambda a,b : a<=b,
                'COMP_INFILE':lambda a,b : b in a}
        if len(self.filter)==0: return 1
        for filter in self.filter:
            record_data = record[filter['FieldID'].lower()].strip()
            if filter['CompType'] == 'COMP_INFILE':
                if not self.targetrecords:
                    self.targetrecords=self.read_dbf(targetfile)
                FieldValue = [t[filter['TargetFieldID']] for t in self.targetrecords]
            else:
                FieldValue = filter['FieldValue']
            # if filter['Type'].upper() == 'STRING':
            #     filter['FieldValue'] = "'%s'"%filter['FieldValue']
            if filter['LinkType'].upper() == 'AND':
                if not comp[filter['CompType']](FieldValue,record_data):return -1
            if filter['LinkType'].upper() == 'OR':
                if comp[filter['CompType']](FieldValue,record_data):return 1
        if filter['LinkType'].upper() == 'AND':
            return 1
        if filter['LinkType'].upper() == 'OR':
            return -1

    def get_txt_comp_result(self,record):
        comp = {'COMP_EQUAL':lambda a,b : a==b,
                'COMP_NOTEQUAL':lambda a,b : a!=b,
                'COMP_LESS':lambda a,b : a<b,
                'COMP_NOTLESS':lambda a,b : a>=b,
                'COMP_GREAT':lambda a,b : a>b,
                'COMP_NOTGREAT':lambda a,b : a<=b}
        if len(self.filter)==0: return 1
        try:
            data_tuple = re.split(self.filterflag, record)
            if len(data_tuple)<2: return -1
        except:
            return -1
        for filter in self.filter:
            record_data = data_tuple[int(filter.get('FieldID'))-1]
            if 'A459366481' in record:
                self.log.debug(data_tuple)
                self.log.debug('FieldID:%s'%(filter['FieldID']))
                self.log.debug('record_data:%s'%record_data)
            # if filter['Type'].upper() == 'STRING':
            #     filter['FieldValue'] = "'%s'"%filter['FieldValue']
            if filter['LinkType'].upper() == 'AND':
                if not comp[filter['CompType']](filter['FieldValue'],record_data):return -1
            if filter['LinkType'].upper() == 'OR':
                if comp[filter['CompType']](filter['FieldValue'],record_data):return 1
        if filter['LinkType'].upper() == 'AND':
            return 1
        if filter['LinkType'].upper() == 'OR':
            return -1

    def read_dbf(self, path=''):
        if len(path) == 0: path = self.filefrom
        # 部分文件不是.dbf结尾，必须先转换成.dbf才能读写
        if '.DBF' not in path.upper(): 
            tmp_path = os.path.abspath('tmp_read/%s.DBF'%os.path.basename(self.filefrom))
            if not os.path.exists('tmp_read'):os.mkdir('tmp_read')
            try:
                shutil.copy(self.filefrom, tmp_path)
                path = tmp_path
            except:
                self.log.error('[%s] %s copy %s to tmp_dbf fail:%s'%(self.id,self.fileid,self.filefrom,tmp_path))
                self.log.trace()
                return False
        records = []
        if os.path.exists(path):
            records = dbf.Table(path)
            records.open()
            if records not in self.dbfs:
                self.dbfs.append(records)
        return records

    def read_txt(self, path=''):
        if len(path) == 0: path = self.filefrom
        try:
            f = open(path, 'r')
            txt = f.read()
            records = re.findall('(.*?\n)',txt)
        except:
            return False
        return records

    def get_dbf_data(self, records):
        # records = self.read_dbf(self.filefrom)
        mydata = [record for record in records if self.get_comp_result(record)==1]
        return mydata

    def get_txt_data(self, records):
        mydata = [record for record in records if self.get_txt_comp_result(record)==1]
        return mydata

    def get_total_records(self):
        tmp_dbf = os.path.abspath('tmp/%s.dbf'%self.id)
        if not os.path.exists('tmp'):os.mkdir('tmp')
        if os.path.exists(tmp_dbf):
            os.remove(tmp_dbf)
        try:
            shutil.copy(self.filefrom, tmp_dbf)
        except:
            pass
        if not os.path.exists(tmp_dbf):
            self.log.error('[%s] %s copy %s to tmp_dbf fail:%s.dbf'%(self.id,self.fileid,self.filefrom,self.id))
            return False
        records = dbf.Table(tmp_dbf)
        records.open()
        if records not in self.dbfs:
            self.dbfs.append(records)
        return records

    def write_local_dbf_by_del(self, records):
        for record in records:
            if self.get_comp_result(record)!=1:
                dbf.delete(record)
        records.pack()
        select_records = len(records)
        records.close()
        self.log.debug('[%s] %s write %s.dbf success'%(self.id,self.fileid,self.id))
        return select_records

    def write_local_txt(self, data_list):
        try:
            new_records = data_list
            tmp_txt = os.path.abspath('tmp/%s.txt'%self.id)
            if not os.path.exists('tmp'):os.mkdir('tmp')
            f = open(tmp_txt,'w')
            for record in new_records:
                f.write(record)
            f.close()
            self.log.debug('[%s] %s write %s.txt success'%(self.id,self.fileid,self.id))
            return True
        except:
            return False

    def write_local_dbf_by_append(self,data_list):
        new_records = data_list
        # if not new_records: return False
        tmp_dbf = os.path.abspath('tmp/%s.dbf'%self.id)
        if not os.path.exists('tmp'):os.mkdir('tmp')
        try:
            modelpath = os.path.abspath('dbfmodel/%s'%os.path.basename(self.filefrom))
            # 部分文件不是.dbf结尾，必须先转换成.dbf才能读写
            if not os.path.exists('dbfmodel'):os.mkdir('dbfmodel')
            if '.DBF' not in modelpath.upper(): modelpath += '.DBF'
            self.log.debug('task%s:modelpath:%s'%(self.id,modelpath))
            if 'Y' in self.config.get('newmodel','no').upper():
                os.remove(modelpath)
            if os.path.exists(modelpath):
                shutil.copy(modelpath, tmp_dbf)
            else:
                # 要删除，此为创建模板用
                shutil.copy(self.filefrom, modelpath)
                records = dbf.Table(modelpath)
                records.open()
                for record in records:
                    dbf.delete(record)
                records.pack()
                records.close()
                shutil.copy(modelpath, tmp_dbf)
        except:
            self.log.trace()
            self.log.error('[%s] %s copy %s to dbfmodel fail:%s'%(self.id,self.fileid,self.filefrom,modelpath))
        if not os.path.exists(tmp_dbf):
            self.log.error('[%s] %s copy %s to tmp_dbf fail:%s.dbf'%(self.id,self.fileid,self.filefrom,self.id))
            return False
        records = dbf.Table(tmp_dbf)
        records.open()
        if records:
            for record in records:
                dbf.delete(record)
            records.pack()
        for record in new_records:
            records.append(record)
        records.close()
        self.log.debug('[%s] %s write %s.dbf success'%(self.id,self.fileid,self.id))
        return True

    def send_ok_file(self):
        # 此函数有问题！！目的地多个ok文件只有发送一个
        # 暂时不处理了，因为现在改用点击按钮后动发送
        if 'N' in self.config.get('okfile','no').upper(): return True
        if 'N' in self.config.get("copyresult",'yes').upper(): return True
        tmp_ok_file = os.path.abspath('tmp/ok.ok')
        if not os.path.exists(tmp_ok_file):
            f=open(tmp_ok_file,'w')
            f.write('')
            f.close()
        result = True
        for fileto in self.fileto:
            ok_file = fileto.get('SaveName','')
            if '.ok' not in ok_file.lower(): ok_file += '.ok'
            self.log.debug('ok file:%s'%ok_file)
            try:
                shutil.copy(tmp_ok_file, ok_file)
            except:
                pass
            # os.system(r'copy %s %s'%(tmp_ok_file, ok_file))
            # result = tools.command_run(r'copy %s %s'%(tmp_ok_file, ok_file), 3)
            if not os.path.exists(ok_file):
                self.log.error('[%s] %s copy %s to destination:%s fail'%(self.id,self.fileid,tmp_ok_file,ok_file))
                result = False
            self.log.debug('[%s] %s copy %s to destination:%s success'%(self.id,self.fileid,tmp_ok_file,ok_file))
        return result

    def copy_to_destination(self):
        if 'N' in self.config.get("copyresult",'yes').upper():
            return True
        if self.iftxt:
            tmp_file = os.path.abspath('tmp/%s.txt'%self.id)
        else:
            tmp_file = os.path.abspath('tmp/%s.dbf'%self.id)
        for destination in self.fileto:
            self.log.debug(r'copy %s %s'%(tmp_file, destination.get('SaveName','')))
            # os.system(r'copy %s %s'%(tmp_file, ok_file))
            try:
                shutil.copy(tmp_file, destination.get('SaveName',''))
            except:
                pass
            # result = tools.command_run(r'copy %s %s'%(tmp_file, destination.get('SaveName','')), 3)
            if not os.path.exists(destination.get('SaveName','')):
                self.log.error('[%s] %s copy %s.dbf to destination:%s fail'%(self.id,self.fileid,self.id,destination.get('SaveName','')))
                return False
            self.log.info('[%s] %s copy %s.dbf to destination:%s success'%(self.id,self.fileid,self.id,destination.get('SaveName','')))
        return True

    def work_txt(self):
        if self.check_data():
            self.log.debug("[%s] %s check_data ok" %(self.id,self.fileid))
            total_records = task.read_txt()
            self.log.info('[%s] %s 总记录 %s'%(self.id,self.fileid,len(total_records)))
            mydata = task.get_txt_data(total_records)
            self.log.info('[%s] %s 匹配 %s'%(self.id,self.fileid,len(mydata)))
            if mydata:
                if not task.write_local_txt(mydata):
                    task.log.debug("[%s] %s write_local_txt fail" %(task.id,task.fileid))
                    return False
                if self.copy_to_destination():
                    if self.send_ok_file():
                        self.log.info('[%s] %s 执行完成'%(self.id,self.fileid))
                        return True

    def work_nofilter(self):
        for destination in self.fileto:
            self.log.debug(r'copy %s %s'%(self.filefrom, destination.get('SaveName','')))
            # os.system(r'copy %s %s'%(tmp_file, ok_file))
            try:
                shutil.copy(self.filefrom, destination.get('SaveName',''))
            except Exception as e:
                self.log.error('[%s] %s copy to %s fail:%s'%(self.id,self.fileid,destination.get('SaveName',''),e))
            # result = tools.command_run(r'copy %s %s'%(tmp_file, destination.get('SaveName','')), 3)
            if not os.path.exists(destination.get('SaveName','')):
                self.log.error('[%s] %s copy %s.dbf to destination:%s fail'%(self.id,self.fileid,self.id,destination.get('SaveName','')))
                return False
            self.log.info('[%s] %s copy %s.dbf to destination:%s success'%(self.id,self.fileid,self.id,destination.get('SaveName','')))
        return True
        
    def work_new(self):
        self.msg_queue.put(('process_progress',(self.id, 5)))
        if self.check_data():
            self.log.debug("task%s:%s check_data ok" %(self.id,self.fileid))
            self.msg_queue.put(('process_progress',(self.id, 10)))
            t1 = time.time()
            self.log.info('[%s] ---------------------'%self.id)
            if len(self.filter)==0:
                self.log.info('[%s] 无过滤条件,直接拷贝开始'%(self.id))
                if self.work_nofilter():
                    self.msg_queue.put(('process_progress',(self.id, 100)))
                    self.log.info('[%s] 拷贝结束,当前状态:成功, 耗时:%f'%(self.id,time.time()-t1))
                    return True
                else:
                    self.msg_queue.put(('process_progress',(self.id, 12)))
                    self.log.info('[%s] 拷贝结束,当前状态:失败, 耗时:%f'%(self.id,time.time()-t1))
                    return False
            self.log.info('[%s] 开始调用C模块拆分dbf'%(self.id))
            try:
                # import pprint
                # pprint.pprint(self.data)
                result = pythondemo.work(self.data) 
                if result:
                    status,total,select,logList = result
                    t2 = time.time()
                    for l in logList:
                        level, msg = l
                        self.log.log(level,'[%s] %s'%(self.id,msg))
                    # self.log.info('[%s] 调用C模块拆分dbf结束,当前状态:%d,total:%d,select:%d, 耗时:%f'%(self.id,
                    self.log.info('[%s] 调用C模块拆分dbf结束,当前状态:%s,total:%d,select:%d, 耗时:%f'%(self.id, _status[status], total, select, t2-t1))
                    if self.msg_queue:
                        self.msg_queue.put(('process_filter_records',(self.id, select)))
                        self.msg_queue.put(('process_total_records',(self.id, total)))
                        self.msg_queue.put(('process_progress',(self.id, status)))
                    if status==100:
                        return True
                    else:
                        return False
                else:
                    self.log.warning('[%s] 调用C模块结果为空!'%self.id)
            except Exception as e:
                self.log.error('[%s] 任务异常!%s'%(self.id,e))

    def work(self):
        if self.check_data():
            self.log.debug("task%s:%s check_data ok" %(self.id,self.fileid))
            total_records = self.read_dbf(self.filefrom)
            self.log.info('task%s:%s 总记录 %s'%(self.id,self.fileid,len(total_records)))
            mydata = self.get_dbf_data(total_records)
            self.log.info('task%s:%s 匹配 %s'%(self.id,self.fileid,len(mydata)))
            if self.write_local_dbf_by_append(mydata):
                self.log.info("task%s:%s write_local_dbf ok" %(self.id,self.fileid))
                if self.copy_to_destination():
                    if self.send_ok_file():
                        self.log.info('task%s:%s 执行完成'%(self.id,self.fileid))
                        return True
        return False

if __name__ == '__main__':
    import myxml2 as myxml
    import log
    import time
    import pythondemo
    import pprint
    config = myxml.get_sysconfig_from_xml()
    loglevel = config.get('loglevel','DEBUG')
    mytime = config.get('sysdate', '')
    data = myxml.get_task_from_xml(sysdate=mytime)
    data = myxml.get_task_from_xml(sysdate='20170206')
    today = time.localtime(time.time())
    today = time.strftime("%Y%m%d", today)
    mylog = log.Log(filename='log/all.%s.log'%today,cmdlevel=loglevel)
    err_log = log.Log('error',filename='error/err.%s.log'%today,filelevel='error')
    # err_log = log.Log('error',filename='error/err.%s.log'%today,filelevel='error',backup_count=1,when='D')
    mylog = mylog.addFileLog(err_log)
    # mylog.debug(data[0])
    st = time.time()
    success = 0
    # ********************************************
    for t in data:
        if t["attrib"]['FileID']=='JSMX_SJKY1_RZRQ':
            pprint.pprint(t)
            os.system("pause")
            result = pythondemo.work(data=t)
            break
    if result:
        print(type(result))
        print(result)
    print(time.time()-st)
    os.system("pause")
    # ********************************************
    for item in data:
        task = Task(mylog,config,item)
        # if task.work():
        if task.work_txt():
            success += 1
        mylog.debug('task %s time used %s'%(task.id, time.time()-t))
        t = time.time()
    mylog.info('-'*25)
    mylog.info("total:%s, success:%s, fail:%s"%(len(data),success,len(data)-success))
    mylog.info('-'*25)
    if 'N' in config.get('autorun', 'yes').upper():
        os.system("pause")


