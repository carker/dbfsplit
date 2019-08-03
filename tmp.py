# import shutil
# for i in range(10):
#     shutil.copy("\\\\192.101.1.53\\d$\\20170711\\sjsgb.dbf",
#         "\\\\192.101.1.53\\d$\\20170711\\sjsgb_%s.dbf"%i)
f = open('config.xml','a')
for i in range(7,199):
    txt='''  <DBFFile FileID="JSMX_SJKY1_RZRQ11" Description="">
      	<Source      Description="" FileName="\\\\192.101.1.53\d$\\20170711\sjsgb_%d.dbf"/>
      	<Destination Description="" SaveName="\\\\192.101.1.53\d$\\20170711\d\hehe%d.dbf"/>
      	<Filter Description="" FilterFlag="\s+" TargetFile="\\\\127.0.0.1\h$\onedrive\python\python3\dbfsplit\ZSMXFK@M@D.DBF">
      		<Field FieldID="gbdm1" FieldName="" FieldValue="116077" Type="string" CompType="COMP_EQUAL" LinkType="AND"/>
      	</Filter>
      </DBFFile>   
    '''%(i,i)
    f.write(txt)
f.close()
