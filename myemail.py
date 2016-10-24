import smtplib
from email.mime.text import MIMEText
from email.header import Header

class Myemail():
    def __init__(self, server, sender, receivers):
        self.server = server
        self.host = server['host']
        self.port = server['port']
        self.sender = sender
        self.sender_addr = sender['address']
        self.user = sender['user']
        self.password = sender['password']
        self.receivers = receivers

    def setServer(self, server):
        # server = {'host':"mail.chinalions.cn",
        #           'port':25}
        self.server = server
        self.host = server['host']
        self.port = server['port']

    def setSender(self, sender):
        # sender = {'address':'jira@chinalions.cn',
        #           'user':'jira',
        #           'password':'aaa123aaa',
        #           'header_from':'系统提醒'}
        self.sender = sender
        self.sender_addr = sender['address']
        self.user = sender['user']
        self.password = sender['password']

    def setReceivers(self, receivers):
        # receivers = ['huangming@chinalions.cn']
        self.receivers = receivers

    def sendmail(self, subject, msg):
        message = MIMEText(msg, 'plain', 'utf-8')
        message['Subject'] = Header(subject, 'utf-8')
        message['From'] = Header(self.sender.get('header_from','系统提醒'), 'utf-8')
        message['To'] = Header(','.join(self.receivers), 'utf-8')
        try:
            smtpObj = smtplib.SMTP() 
            smtpObj.connect(self.host, self.port)    # 25 为 SMTP 端口号
            smtpObj.login(self.user,self.password)  
            smtpObj.sendmail(self.sender_addr, self.receivers, message.as_string())
            return 'success'
        except smtplib.SMTPException as err:
            return err

if __name__ == '__main__':
    server = {'host':"mail.chinalions.cn",
              'port':25}
    receivers = ['huangming@chinalions.cn']
    sender = {'address':'jira@chinalions.cn',
              'user':'jira',
              'password':'aaa123aaa',
              'header_from':'系统提醒'}
    msg = 'Python SMTP 邮件'
    subject = 'Python 邮件发送'
    myemail = Myemail(server, sender, receivers)
    myemail.sendmail(subject, msg)

