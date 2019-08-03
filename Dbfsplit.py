from PyQt5 import QtCore, QtWidgets
from PyQt5.QtGui import QColor, QStandardItemModel
from PyQt5.QtWidgets import QStyle, QItemDelegate,QStyleOptionProgressBar,QHeaderView
from PyQt5.QtWidgets import QItemDelegate, QWidget, QStyleOptionViewItem, QCheckBox
from PyQt5.QtCore import QModelIndex,QVariant
from Ui_Dbfsplit import Ui_MainWindow
# import myxml
import myxml2 as myxml
from myemail import Myemail
import base64
import time
import sys
import os
import shutil
import log
from work_thread import Work_Thread
import datetime

from work import Task
from process_msg_thread import Msg_Thread
from functools import partial
from multiprocessing.pool import Pool
from multiprocessing import Process, Manager

def do_works(*args,**kwds):
    # mylog = kwds.get('log',None)
    config = kwds.get('config',{})
    task_list = kwds.get('task_list')
    process_id = kwds.get('id')

    # loglevel = config.get('loglevel', 'DEBUG')
    # today = time.localtime(time.time())
    # today = time.strftime("%Y%m%d", today)
    # mylog = log.Log(filename='log/process_all.%s.log'%today,cmdlevel=loglevel)
    # err_log = log.Log('error',filename='error/process_err.%s.log'%today,filelevel='error')
    # mylog = mylog.addFileLog(err_log)
    # if not mylog: mylog = log.Log()

    msg_queue = args[0]
    mylog = ProcessLog(msg_queue)
    mylog.debug('进程开始:%s'%process_id)
    success = 0
    for item in task_list:
        try:
            mylog.debug('task%s:%s开始'%(item['id'],item['source']['FileName']))
            task = Task(mylog, config, item, msg_queue)
            tmp_dict = {}
            if task.iftxt:
                if split_txt(tmp_dict,msg_queue,task):
                    success += 1
            else:
                # if split_dbf_by_append(tmp_dict,msg_queue,task):
                if task.work_new():
                    success +=1
        except:
            mylog.error('task%s:%s未知错误%s'%(item['id'],item['source']['FileName'],sys.exc_info()[:2]))
    msg_queue.put(('process_end',(process_id, success)))
    mylog.info('--------------------------')
    mylog.info('进程[%s]执行完成,成功：%s'%(process_id,success))

def split_dbf_by_append(tmp_dict,msg_queue,task):
    msg_queue.put(('process_progress',(task.id, 5)))
    if task.check_data():
        task.log.debug("task%s:%s check_data ok" %(task.id,task.fileid))
        msg_queue.put(('process_progress',(task.id, 10)))
        if tmp_dict.get('total_records'):
            total_records = tmp_dict.get('total_records')
            total_records_num = tmp_dict.get('total_records_num')
        else:
            total_records = task.read_dbf()
            if total_records == False: return False
            total_records_num = len(total_records)
            tmp_dict['total_records'] = total_records
            tmp_dict['total_records_num'] = total_records_num
        task.log.info('task%s:%s 总记录 %s'%(task.id,task.fileid,total_records_num))
        msg_queue.put(('process_progress',(task.id, 30)))
        msg_queue.put(('process_total_records',(task.id, total_records_num)))
        select_records = task.get_dbf_data(total_records)
        select_records_num = len(select_records)
        if not task.write_local_dbf_by_append(select_records):
            task.log.debug("task%s:%s write_local_dbf fail" %(task.id,task.fileid))
            return False
        msg_queue.put(('process_progress',(task.id, 60)))
        msg_queue.put(('process_filter_records',(task.id, select_records_num)))
        task.log.info('task%s:%s 匹配 %s'%(task.id,task.fileid,select_records_num))
        task.log.debug("task%s:%s write_local_dbf ok" %(task.id,task.fileid))
        if task.copy_to_destination():
            task.log.info('task%s:%s 执行完成'%(task.id,task.fileid))
            msg_queue.put(('process_progress',(task.id, 100)))
            return True
            if task.send_ok_file():
                task.log.info('task%s:%s 执行完成'%(task.id,task.fileid))
                msg_queue.put(('process_progress',(task.id, 100)))
                return True
    return False

def split_txt(tmp_dict,msg_queue,task):
    msg_queue.put(('process_progress',(task.id, 5)))
    if task.check_data():
        task.log.debug("[%s] %s check_data ok" %(task.id,task.fileid))
        msg_queue.put(('process_progress',(task.id, 10)))
        if tmp_dict.get('total_records'):
            total_records = tmp_dict.get('total_records')
            total_records_num = tmp_dict.get('total_records_num')
        else:
            total_records = task.read_txt()
            if total_records == False: return False
            total_records_num = len(total_records)
            tmp_dict['total_records'] = total_records
            tmp_dict['total_records_num'] = total_records_num
        task.log.info('[%s] %s 总记录 %s'%(task.id,task.fileid,total_records_num))
        msg_queue.put(('process_progress',(task.id, 30)))
        msg_queue.put(('process_total_records',(task.id, total_records_num)))
        select_records = task.get_txt_data(total_records)
        select_records_num = len(select_records)
        if not task.write_local_txt(select_records):
            task.log.debug("[%s] %s write_local_dbf fail" %(task.id,task.fileid))
            return False
        msg_queue.put(('process_progress',(task.id, 60)))
        msg_queue.put(('process_filter_records',(task.id, select_records_num)))
        task.log.info('[%s] %s 匹配 %s'%(task.id,task.fileid,select_records_num))
        task.log.debug("[%s] %s write_local_dbf ok" %(task.id,task.fileid))
        if task.copy_to_destination():
            task.log.info('[%s] %s 执行完成'%(task.id,task.fileid))
            msg_queue.put(('process_progress',(task.id, 100)))
            return True
            if task.send_ok_file():
                task.log.info('[%s] %s 执行完成'%(task.id,task.fileid))
                msg_queue.put(('process_progress',(task.id, 100)))
                return True
    return False

class ProcessLog():
    def __init__(self,queue):
        self.queue = queue

    def log(self,loglevel,msg):
        # print('发送%s %s'%(loglevel,msg))
        self.queue.put(('log',(loglevel, msg)))

    def debug(self, msg):
        self.log('debug', msg)
    def info(self, msg):
        self.log('info', msg)
    def warning(self, msg):
        self.log('warning', msg)
    def error(self, msg):
        self.log('error', msg)
    def critical(self, msg):
        self.log('critical', msg)
    def exception(self, msg):
        self.log('exception', msg)

    # def debug(self,msg):
    #     msg_queue.put({'log':('debug', msg)})

class CheckBoxDelegate(QtWidgets.QItemDelegate):

    def __init__(self, parent=None):
        QItemDelegate.__init__(self, parent)
        self.chkboxSize = 19 #?!

    def createEditor(self, parent, option, index):
        chkbox = QtWidgets.QCheckBox(parent)
        chkbox.setText('')
        chkbox.setTristate(False) #只用两个状态
        left = option.rect.x() + (option.rect.width() - self.chkboxSize) / 2
        top  = option.rect.y() + (option.rect.height() - self.chkboxSize) / 2
        chkbox.setGeometry(left, top, self.chkboxSize, self.chkboxSize)
        return chkbox

    def paint(self, painter, option, index):
        value = bool(index.data())
        # value = False
        opt = QtWidgets.QStyleOptionButton()
        opt.state |= QStyle.State_Enabled | (QStyle.State_On if value else QStyle.State_Off)
        opt.text = ''
        left = option.rect.x() + (option.rect.width() - self.chkboxSize) / 2
        top  = option.rect.y() + (option.rect.height() - self.chkboxSize) / 2
        opt.rect = QtCore.QRect(left, top, self.chkboxSize, self.chkboxSize)
        QtWidgets.QApplication.style().drawControl(QStyle.CE_CheckBox, opt, painter)

    def updateEditorGeometry(self, editor, option, index):
        pass


class ProgressDelegate(QItemDelegate):
    def __init__(self,parent=None):
        QItemDelegate.__init__(self, parent)
    
    def paint(self, *args, **kwargs):
        column = args[2].column()
        value = args[2].data()
        if column == 2:
            progressBarOption = QStyleOptionProgressBar()
            progressBarOption.rect = args[1].rect
            progressBarOption.minimum = 0
            progressBarOption.maximum = 100
            if isinstance(value, int) : 
                progressBarOption.progress = int(value)
                progressBarOption.text =  "{0}%".format(int(value))
            progressBarOption.textVisible = True
            QtWidgets.QApplication.style().drawControl(QStyle.CE_ProgressBar, progressBarOption, args[0], None)
            return
        return QItemDelegate.paint(self, *args, **kwargs)

class MainWindow(QtWidgets.QMainWindow, Ui_MainWindow):
    def __init__(self, parent = None,log=None,config={},data=[]):
        super(MainWindow, self).__init__(parent)
        self.setupUi(self)
        self.config = myxml.config
        self.init()
        self.data = data
        self.log = log
        self.init_sysdate2today()
        self.model = QStandardItemModel(len(self.data), 10, self.task_QTableView)
        self.init_task_frame(data)
        self.signl_connect()
        self.msg_thread = None # 多进程获取消息线程
        self.p = None
        # self.threadnum = int(self.config.get('threadnum', '10'))
        self.threadnum = self.threadnum_spinBox.value()
        self.thread_list = []  # task按源文件分成多个线程
        if self.newmodel_checkBox.isChecked():
            self.config.update({'newmodel':'yes'})
        else:
            self.config.update({'newmodel':'no'})
        self.show()
        if 'Y' in self.config.get('autorun', 'Y').upper():
            self.on_run_pushButton_clicked()

    def createdirs(self,task_list):
        if not os.path.exists('tmp'):os.mkdir('tmp')
        if not os.path.exists('tmp_read'):os.mkdir('tmp_read')
        if not os.path.exists('dbfmodel'):os.mkdir('dbfmodel')
        if 'Y' in self.config.get("copyresult",'yes').upper():
            for task in task_list:
                for destination in task['destination']:
                    dir_path,_ = os.path.split(destination.get('SaveName'))
                    try:
                        if not os.path.exists(dir_path): os.makedirs(dir_path)
                        self.log.debug("创建目的路径%s成功"%dir_path)
                    except:
                        self.log.error("创建目的路径%s失败"%dir_path)
                        self.log.trace()

    def init(self):
        self.threadnum = self.threadnum_spinBox.value()
        if self.newmodel_checkBox.isChecked():
            self.config.update({'newmodel':'yes'})
        else:
            self.config.update({'newmodel':'no'})
        self.msg_label.setText('')
        self.thread_result = {}  # taskid:success
        self.thread = []         # 每启动一个线程就放进来，用于停止所有线程操作
        self.task_select = []    # 存储选中的task

    def signl_connect(self):
        self.sysdate_dateEdit.dateChanged.connect(self.load_xml)

    def load_xml(self):
        sysdate = self.sysdate_dateEdit.date().toString("yyyyMMdd")
        self.config = myxml.get_sysconfig_from_xml('config.xml')
        self.data = myxml.get_task_from_xml('config.xml', sysdate=sysdate)
        self.init_task_frame(self.data)

    def init_sysdate2today(self):
        today = datetime.datetime.now()
        self.sysdate_dateEdit.setDateTime(today)

    def init_task_frame3(self,task_list=[]):
        table = self.task_QTableView
        # table = QtWidgets.QTableView()
        model = QStandardItemModel(3, 3, table)
        model.setHorizontalHeaderLabels(['Name', 'Description', 'Animal?'])
        model.setData(model.index(0, 0, QModelIndex()), QVariant('Squirrel'))
        model.setData(model.index(0, 1, QModelIndex()), QVariant(u'可爱的松树精灵'))
        model.setData(model.index(0, 2, QModelIndex()), QVariant(True))
        model.setData(model.index(1, 0, QModelIndex()), QVariant('Soybean'))
        model.setData(model.index(1, 1, QModelIndex()), QVariant(u'他站在田野里吹风'))
        model.setData(model.index(1, 2, QModelIndex()), QVariant(False))
        table.setModel(model)
        self.verticalLayout_2.addWidget(table)
        # table.setItemDelegateForColumn(2, CheckBoxDelegate(table))
        # table.resizeColumnToContents(1)
        # table.horizontalHeader().setStretchLastSection(True)

    def init_task_frame(self,task_list=[]):
        if len(task_list)==0:task_list = self.data
        self.model.setHorizontalHeaderLabels(['任务ID','是否启用','处理进度','OK文件','匹配数',
                                              '总记录数','文件编码','文件描述','文件名称','保存名称'])
        for i in range(len(task_list)):
            task = task_list[i]
            taskid = task['id']
            fileid = task['attrib']['FileID']
            filename = task['attrib']['Description']
            source = task['source']['FileName']
            destination = ''
            for des in task['destination']:
                destination += '%s\n'%des['SaveName']
            self.model.setData(self.model.index(i,0,QModelIndex()),QVariant(taskid))
            self.model.item(i,0).setTextAlignment(QtCore.Qt.AlignCenter)
            self.model.setData(self.model.index(i,1,QModelIndex()),QVariant(True))
            self.model.setData(self.model.index(i,2,QModelIndex()),QVariant(0))
            self.model.setData(self.model.index(i,3,QModelIndex()),QVariant(''))
            self.model.setData(self.model.index(i,4,QModelIndex()),QVariant(''))
            self.model.setData(self.model.index(i,5,QModelIndex()),QVariant(''))
            self.model.setData(self.model.index(i,6,QModelIndex()),QVariant(fileid))
            self.model.setData(self.model.index(i,7,QModelIndex()),QVariant(filename))
            self.model.setData(self.model.index(i,8,QModelIndex()),QVariant(source))
            self.model.setData(self.model.index(i,9,QModelIndex()),QVariant(destination[:-1]))

        self.task_QTableView.setModel(self.model)
        self.task_QTableView.setItemDelegateForColumn(1, CheckBoxDelegate(self.task_QTableView))
        self.task_QTableView.setItemDelegateForColumn(2, ProgressDelegate(self.task_QTableView))
        self.task_QTableView.horizontalHeader().setStretchLastSection(True)
        self.task_QTableView.resizeColumnsToContents()
        self.task_QTableView.resizeRowsToContents()

    def get_select_task(self):
        self.task_select = []
        for task in self.data:
            if self.model.data(self.model.index(task['id'],1)):
                self.task_select.append(task)
        return self.task_select

    def sort_data(self,task_list):
        dict_list = {}
        for task in task_list:
            if task['source']['FileName'] not in dict_list:
                # task_dict = {task['source']['FileName']:[task]}
                dict_list[task['source']['FileName']] = [task]
            else:
                new_list = dict_list[task['source']['FileName']]
                new_list.append(task)
                dict_list[task['source']['FileName']] = new_list
        return dict_list

    def sendmail(self):
        server = {'host':self.config.get('email_sender_host', ''),
                  'port':int(self.config.get('email_sender_port', '25'))}
        receivers = self.config.get('email_receivers', '')
        sender = {'address':self.config.get('email_sender_adress', ''),
                  'user':self.config.get('email_sender_user', ''),
                  'password':base64.b64decode(self.config.get('email_sender_password', '')[::-1]).decode(),
                  'header_from':'系统提醒'}
        subject = 'DBF分拆提醒'
        if 'y' in self.config.get('email', '').lower():
            try:
                total_success = sum([y for (x,y) in self.thread_result.items() if y >0])
                myemail = Myemail(server, sender, receivers)
                today = time.localtime(time.time())
                today = time.strftime("%Y%m%d", today)
                filename='error/err.%s.log'%today
                msg = open(filename).read()
                msg = '任务完成！%s个任务，成功: %s个\n\n----------------\n%s'%(len(self.task_select), total_success, msg)
                result = myemail.sendmail(subject, msg)
                self.log.debug('send email:%s'%result)
            except:
                self.log.trace()

    def create_msg_thread(self, log, queue, threadid="日志线程"):
        msg_thread = Msg_Thread(log, queue)
        msg_thread.setIdentity(threadid)
        msg_thread.setDaemon(True)
        msg_thread.msg_update_progress.connect(self.update_progress)
        msg_thread.msg_total_records.connect(self.update_total_records)
        msg_thread.msg_filter_records.connect(self.update_filter_records)
        msg_thread.msg_thread_end.connect(self.process_end) # 跟线程结束处理有冲突，重写处理
        # self.thread_result[threadid] = -2
        return msg_thread

    @QtCore.pyqtSlot()
    def on_run_pushButton_clicked(self):
        # QtWidgets.QMessageBox.critical(self, "Critical", u"错误:正则表达式错误")
        self.init()
        select_task = self.get_select_task()
        # self.model.setData(self.model.index(5,1),False)
        # return
        self.createdirs(select_task)
        self.thread_list = [(source,task_thread) for source,task_thread in self.sort_data(select_task).items()]
        self.log.debug('len thread_list:%s'%len(self.thread_list))
        p = Pool()
        self.p=p
        manager = Manager()
        q = manager.Queue()
        task_num = len(self.thread_list) 
        for i in range(task_num):
            if len(self.thread_list)>0:
                source,task_list = self.thread_list.pop(0)
                self.thread_result[source] = -2
                p.apply_async(do_works, args = (q,), kwds = {"id":source,"queue":q,"config":self.config,"task_list":task_list})
                self.log.debug('添加进程成功 %s'%source)
        self.msg_thread = self.create_msg_thread(self.log, q)
        self.msg_thread.start()

        # for i in range(5):
        #     p = Process(target = do_works, kwds = {"config":self.config,"log":self.log,"task_list":self.thread_list.pop(0)[1]})
        #     p.start()
        # if self.check_thread_end():
        #     for i in range(self.threadnum):
        #         if self.thread_list:
        #             source,task_thread = self.thread_list.pop()
        #             work = self.work(task_thread,source)
        #             work.start()
        self.log.debug('end')


    def sendokfile(self,task):
        tmp_ok_file = os.path.abspath('tmp/ok.ok')
        if not os.path.exists(tmp_ok_file):
            f=open(tmp_ok_file,'w')
            f.write('')
            f.close()
        for fileto in task['destination']:
            ok_file = fileto.get('SaveName','')
            if '.ok' not in ok_file.lower(): ok_file += '.ok'
            self.log.debug('ok file:%s'%ok_file)
            try:
                shutil.copy(tmp_ok_file, ok_file)
            except:
                self.log.trace()
            # os.system(r'copy %s %s'%(tmp_ok_file, ok_file))
            # result = tools.command_run(r'copy %s %s'%(tmp_ok_file, ok_file), 3)
            if not os.path.exists(ok_file):
                self.log.error('task%s:%s copy okfile to destination:%s fail'%(task['id'],task['attrib'].get('FileID',''),ok_file))
                return False
            self.log.debug('task%s:%s copy okfile to destination:%s success'%(task['id'],task['attrib'].get('FileID',''),ok_file))
        return True

    @QtCore.pyqtSlot()
    def on_sendok_PushButton_clicked(self):
        self.log.debug('sendok button click')
        select_task = self.get_select_task()
        self.createdirs(select_task)
        for task in select_task:
            taskid = task['id']
            self.statusBar().showMessage('任务[%s]正在发送OK文件'%(self.model.data(self.model.index(taskid,0))))
            result = self.sendokfile(task)
            self.log.debug(result)
            self.update_okflag(task, result)
            flag = '成功' if result else '失败'
            self.statusBar().showMessage('任务[%s]发送OK文件%s'%(self.model.data(self.model.index(taskid,0)), flag))

    def update_okflag(self, task, result):
        try:
            row = int(task['id'])
            flag = '√' if result else 'x'
            self.model.setData(self.model.index(row,3),QVariant(flag))
        except:
            self.log.trace()
    
    def reset(self):
        rowmax = self.model.rowCount()
        for rowcount in range(rowmax):
            self.model.setData(self.model.index(rowcount,2),QVariant(0))
            self.model.setData(self.model.index(rowcount,3),QVariant(''))
            self.model.setData(self.model.index(rowcount,4),QVariant(''))
            self.model.setData(self.model.index(rowcount,5),QVariant(''))
        self.statusBar().showMessage('')

    @QtCore.pyqtSlot()
    def on_reset_pushButton_clicked(self):
        self.reset()

    @QtCore.pyqtSlot()
    def on_stop_pushButton_clicked(self):
        self.msg_label.setText('正在停止线程...')
        if self.p:self.p.terminate()
        for thread in self.thread:
            thread.stop()

    @QtCore.pyqtSlot()
    def on_exit_pushButton_clicked(self):
        # self.init_sysdate()
        self.close()

    @QtCore.pyqtSlot()
    def on_errorlog_PushButton_clicked(self):
        try:
            today = time.localtime(time.time())
            today = time.strftime("%Y%m%d", today)
            error_log_path = os.path.abspath('error/err.%s.log'%today)
            if os.path.exists(error_log_path):
                os.popen('notepad %s'%error_log_path)
        except:
            self.log.trace()

    @QtCore.pyqtSlot()
    def on_select_pushButton_clicked(self):
        rowmax = self.model.rowCount()
        for rowcount in range(rowmax):
            self.model.setData(self.model.index(rowcount,1),QVariant(True))

    @QtCore.pyqtSlot()
    def on_unselect_pushButton_clicked(self):
        rowmax = self.model.rowCount()
        for rowcount in range(rowmax):
            self.model.setData(self.model.index(rowcount,1),QVariant(False))

    @QtCore.pyqtSlot(tuple)
    def update_progress(self, data_tuple):
        status = {5:'任务开始',10:'参数校验成功',12:'无过滤条件任务拷贝失败',
                 15:'CPP加载参数成功',20:'CPP解析参数成功',25:'CPP读取源文件dbf成功',
                 30:'读取源文件dbf成功',40:'CPP获取过滤数据成功',60:'写本地dbf成功',
                 90:'CPP写目的dbf成功',100:'拷贝到目的地成功,任务结束'}
        taskid, num = data_tuple
        self.model.setData(self.model.index(taskid,2),QVariant(num))
        self.statusBar().showMessage('线程[%s]正在处理: 任务%s,当前进程状态%s'%(self.model.data(self.model.index(int(taskid),8)), taskid, status.get(num,str(num))))
        if num in (12,15,20,25,40,90):
            self.model.item(int(taskid), 6).setBackground(QColor('red'))
        else:
            self.model.item(int(taskid), 6).setBackground(QColor(255,255,255,0))

    @QtCore.pyqtSlot(tuple)
    def update_total_records(self, data_tuple):
        taskid, num = data_tuple
        self.log.debug('[thread msg] total records:%s,%s'%data_tuple)
        self.model.setData(self.model.index(int(taskid),5),QVariant(num))

    @QtCore.pyqtSlot(tuple)
    def update_filter_records(self, data_tuple):
        taskid, num = data_tuple
        self.log.debug('[thread msg] filter records:%s,%s'%data_tuple)
        self.model.setData(self.model.index(int(taskid),4),QVariant(num))

    @QtCore.pyqtSlot(tuple)
    def process_end(self, data_tuple):
        processid, success = data_tuple
        self.thread_result[processid] = success
        # mylog.error('进程[%s]执行完成,任务成功：%s'%(threadid,success))
        total_success = sum([y for (x,y) in self.thread_result.items() if y>0])
        self.msg_label.setText('已完成: %s个任务'%total_success)
        if self.check_thread_end():
            self.log.debug('[thread end msg] %s success: %s'%data_tuple)
            self.log.info('任务完成！%s个任务，成功: %s个'%(len(self.task_select), total_success))
            if 'Y' in self.config.get('autorun', 'Y').upper():
                self.sendmail()
                self.close()
            else:
                self.msg_label.setText('任务完成！%s个任务，成功: %s个'%(len(self.task_select), total_success))
                self.statusBar().showMessage('结束')
            self.msg_thread.stop()


    @QtCore.pyqtSlot(tuple)
    def work_thread_end(self, data_tuple):
        threadid, success = data_tuple
        self.thread_result[threadid] = success
        # mylog.error('进程[%s]执行完成,任务成功：%s'%(threadid,success))
        if self.msg_label.text() != '正在停止线程...':
            total_success = sum([y for (x,y) in self.thread_result.items() if y>0])
            self.msg_label.setText('已完成: %s个任务'%total_success)
            if len(self.thread_list)>0:
                source,task_thread = self.thread_list.pop()
                work = self.work(task_thread,source)
                work.start()
        else:
            self.statusBar().showMessage('线程[%s]结束'%threadid)
        if self.check_thread_end():
            total_success = sum([y for (x,y) in self.thread_result.items() if y >0])
            self.log.debug('[thread end msg] %s success: %s'%data_tuple)
            self.log.debug('任务完成！%s个任务，成功: %s个'%(len(self.task_select), total_success))
            if 'Y' in self.config.get('autorun', 'Y').upper():
                self.sendmail()
                self.close()
            else:
                self.msg_label.setText('任务完成！%s个任务，成功: %s个'%(len(self.task_select), total_success))
                self.statusBar().showMessage('结束')


    def work(self, data, threadid="Work_Thread"):
        work = Work_Thread(self.log, self.config, data)
        work.setIdentity(threadid)
        work.setDaemon(True)
        work.msg_update_progress.connect(self.update_progress)
        work.msg_total_records.connect(self.update_total_records)
        work.msg_filter_records.connect(self.update_filter_records)
        work.msg_thread_end.connect(self.work_thread_end)
        self.thread_result[threadid] = -2
        self.thread.append(work)
        return work

    def check_thread_end(self):
        for thread, success in self.thread_result.items():
            if success < 0 :
                self.log.debug('check_thread_end:%s not end'%thread)
                return False
        return True



if __name__ == '__main__':
    # manager = Manager()
    # q = manager.Queue()
    # mylog = ProcessLog(q)
    # mylog.debug('进程开始')
    # print(a)
    config = myxml.config
    data = myxml.get_task_from_xml('config.xml')
    loglevel = config.get('loglevel', 'DEBUG')
    today = time.localtime(time.time())
    today = time.strftime("%Y%m%d", today)
    mylog = log.Log(filename='log/all.%s.log'%today,cmdlevel=loglevel)
    err_log = log.Log('error',filename='error/err.%s.log'%today,filelevel='error')
    mylog = mylog.addFileLog(err_log)
    app = QtWidgets.QApplication(sys.argv)
    window = MainWindow(data=data, config=config, log=mylog)
    # window.show()
    sys.exit(app.exec_())

