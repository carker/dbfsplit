#!/usr/bin/env python
# -*- coding: utf-8 -*-
#Created Time: 2015/8/25 17:46:50
#author: Cactus
from PyQt5 import QtCore
import threading
import time

class Msg_Thread(threading.Thread, QtCore.QThread):
    msg_update_progress = QtCore.pyqtSignal(tuple)
    msg_total_records = QtCore.pyqtSignal(tuple)
    msg_filter_records = QtCore.pyqtSignal(tuple)
    msg_thread_end = QtCore.pyqtSignal(tuple)

    def __init__(self, log, Queue):
        threading.Thread.__init__(self)
        QtCore.QObject.__init__(self)
        self.timeToQuit = threading.Event()
        self.timeToQuit.clear()
        self.log = log
        self.q = Queue
        self.identity = ''

    def stop(self):
       self.timeToQuit.set()
       self.log.debug('thread %s stop' % self.identity)

    def setIdentity(self, text):
        self.identity = text

    def work_log(self, data):
        try:
            loglevel, msg = data
            func = {'debug':self.log.debug, 'info':self.log.info,
                    'warning':self.log.warning, 'error':self.log.error,
                    'critical':self.log.critical,
                    'exception':self.log.exception}
            func.get(loglevel,self.log.debug)(msg)
        except:
            self.log.trace()

    def work_update_progress(self, data):
        taskid, progress = data
        self.msg_update_progress.emit((taskid, progress))

    def work_update_total_record(self, data):
        self.msg_total_records.emit(data)

    def work_update_filter_record(self, data):
        self.msg_filter_records.emit(data)

    def work_process_end(self, data):
        self.msg_thread_end.emit(data)

    def work(self):
        self.log.debug('thread %s work begin' % self.identity)
        while True:
            try:
                if self.timeToQuit.isSet():
                    self.log.debug('thread %s stop by user' % self.identity)
                    break
                msg_flag, msg_data = self.q.get(True)
                self.log.debug('msg_thread :%s'%str((msg_flag,msg_data)))
                func = {'log':self.work_log,
                        'process_progress':self.work_update_progress,
                        'process_filter_records':self.work_update_filter_record,
                        'process_total_records':self.work_update_total_record,
                        'process_end':self.work_process_end}
                func.get(msg_flag)(msg_data)
                time.sleep(0.05)
            except:
                self.log.trace()
        self.log.debug('thread %s work end' % self.identity)

    def run(self):
        self.log.debug('thread %s begin' % self.identity)
        self.work()

if __name__ == '__main__':
    import log
    import os
    # import myxml
    # config = myxml.get_sysconfig_from_xml()
    # data = myxml.get_task_from_xml()
    import myxml2
    config = myxml2.get_sysconfig_from_xml()
    data = myxml2.get_task_from_xml()
    mylog = log.Log()
    @QtCore.pyqtSlot(tuple)
    def thread_end(data):
        mylog.debug(data)
    @QtCore.pyqtSlot(tuple)
    def update_progress(data):
        mylog.debug(data)
    work = Work_Thread(mylog, config, data)
    work.setIdentity("Work_Thread")
    work.setDaemon(True)
    work.msg_update_progress.connect(update_progress)
    work.msg_thread_end.connect(thread_end)
    work.start()
    from PyQt5 import QtWidgets
    import sys
    app = QtWidgets.QApplication(sys.argv)
    b = QtWidgets.QPushButton(u"你好!")
    b.show()
    app.exec_()
