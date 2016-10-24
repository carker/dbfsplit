import codecs
f = codecs.open('\\\\192.101.1.227\\e$\\huangming\\test.bat','w','utf-8')
txt = '''@echo off
color 0b & cls 

REM ==============================================================================
REM   DBF IMPORT
REM ==============================================================================

:: 设置日期
set aDateTemp=%date%
set aDate=%aDateTemp:~0,4%%aDateTemp:~5,2%%aDateTemp:~8,2%
set aDate=20160930

set aYear=%aDate:~0,4%
set aShortYear=%aDate:~2,2%

set aMonth=%aDate:~4,2%
set aDay=%aDate:~6,2%

:: 执行的sql脚本路径
set sqlpath=%~dp0importdbf.sql
set localsqlserver=127.0.0.1,1433

::DBF相关路径设置 dbfname不带后缀.DBF!!!!!!!!!!!!!!
::set dbfname=SZHK_QTSL%aMonth%.DBF
set dbfname=SZHK_SJSDZ%aMonth%
set dbfpath=\192.101.1.107\d$\LWCSDATA\D-COM\%aDate%set localdbfpath=%temp%

::数据库相关设置
set sqlserver=192.101.1.227,1433
set tmp_link_name=HUANGMING
set sqlserveruser=sa
set sqlserverpsw=newcvs
set dbname=huangming


echo [%date:~0,10% %time%] 开始拷贝DBF文件
echo.

if not exist %localdbfpath%\%dbfname%.DBF (
copy %dbfpath%\%dbfname%.DBF %localdbfpath%\%dbfname%.DBF
)

echo [%date:~0,10% %time%] 拷贝DBF文件完成
echo.

if exist %localdbfpath%\%dbfname%.DBF (
echo [%date:~0,10% %time%] 开始导入DBF到SQLSERVER
echo.
sqlcmd -S "%localsqlserver%" -i %sqlpath%
echo [%date:~0,10% %time%] 导入完成
echo.
)

echo [%date:~0,10% %time%] ===========================================================
echo.


pause

'''
# %s/\\\\/\\\\\\\\/
# %s/\\r/\\\\r/
f.write(txt)
f.close()
