import smtplib
from email.mime.text import MIMEText  
from email.header import Header

class Myemail():
    def __init__(self, server, sender, receivers, ssl=''):
        server_base = {'host':"192.101.1.164",
                       'port':25}
        sender_base = {'address':'123@chinalin.com',
                       'user':'user',
                       'password':'******',
                       'header_from':'系统提醒'}
        self.server = server if server else server_base
        self.sender = sender if sender else sender_base
        self.host = server['host']
        self.port = server['port']
        self.sslPort = server.get('sslPort',465)
        self.sender_addr = sender['address']
        self.user = sender['user']
        self.password = sender['password']
        self.ssl = ssl
        if type(receivers)==type('string'):
            receivers = receivers.split(',')
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
        if type(receivers)==type('string'):
            receivers = receivers.split(',')
        self.receivers = receivers

    def sendmail(self, subject, msg):
        message = MIMEText(msg, 'plain', 'utf-8')
        message['Subject'] = Header(subject, 'utf-8')
        message['From'] = Header(self.sender.get('header_from','系统提醒'), 'utf-8')
        message['To'] = Header(','.join(self.receivers), 'utf-8')
        try:
            if self.ssl=='':
                smtpObj = smtplib.SMTP() 
                smtpObj.connect(self.host, self.port)    # 25 为 SMTP 端口号
                smtpObj.login(self.user,self.password)  
                smtpObj.sendmail(self.sender_addr, self.receivers, message.as_string())
                smtpObj.close()
            elif self.ssl=='tls':
            #tls加密方式，通信过程加密，邮件数据安全，使用正常的smtp端口  
                smtp = smtplib.SMTP(self.host,self.port)  
                smtp.set_debuglevel(True)
                smtp.ehlo()
                smtp.starttls()
                smtp.ehlo()
                smtp.login(self.user,self.password)  
                smtp.sendmail(self.sender_addr, self.receivers, message.as_string())
                smtp.close()
            else:
            #纯粹的ssl加密方式，通信过程加密，邮件数据安全  
                smtp = smtplib.SMTP_SSL(self.host,self.sslPort)  
                smtp.ehlo()  
                smtp.login(self.user,self.password)
                smtp.sendmail(self.sender_addr, self.receivers, message.as_string())
                smtp.close()
            return 'success'
        except smtplib.SMTPException as err:
            return err

if __name__ == '__main__':
    # server = {'host':"mail.chinalin.com",
    server = {'host':"192.101.1.164",
              'port':465}
    receivers = ['huangming@chinalin.com']
    sender = {'address':'huangming@chinalin.com',
              'user':'huangming',
              'password':'hm@123',
              'header_from':'系统提醒'}
    msg = 'Python SMTP 邮件'
    subject = 'Python 邮件发送'
    myemail = Myemail(server, sender, receivers, 'ssl')
    myemail.sendmail(subject, msg)

