import shutil
import os
import base64

dst = "/opt/airflow/imageserver/done"
src = "/opt/airflow/imageserver"


for filename in os.listdir(src):
    f = os.path.join(src, filename)
    print(f)
    if os.path.isfile(f):
        my_string = ""
        with open(f, "rb") as img_file:
            my_string = base64.b64encode(img_file.read())
            my_string = my_string.decode('utf-8')
        with open("/opt/airflow/base64.log","a") as log_file:
            log_file.write(my_string)
            log_file.write("\n")
        shutil.move(f,os.path.join(dst, filename))
