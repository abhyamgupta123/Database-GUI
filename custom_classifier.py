import mysql.connector as connector
from datetime import datetime, date, timedelta
import pandas as pd
from PyQt5.QtGui import *
# from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5 import QtGui, QtCore, uic, QtWidgets
# from PyQt5.QtCore import QCoreApplication, QObject, QThread
# from PyQt5.QtCore import QCoreApplication, pyqtSignal, pyqtSlot
import progress_information_table

class custom_classifier:
    # def __init__(self, connector, fromdate, todate):
    #     self.connector = connector
    #     self.fromdate = fromdate
    #     self.todate = todate


    def __init__(self):
        pass


    def check_duplicate(self, imsi_no):
        query = "SELECT manual_new, manual_absent FROM imsiClassified_manualTable "
        self.cursor1.execute(query)
        data = self.cursor1.fetchall()
        flag = 0

        for row in data:
            if (imsi_no in row ):
                flag = 1
                break
            else:
                continue

        return flag

    def get_imsi_details(self, particular_imsi):
        query = "SELECT first_seen, last_seen FROM " + self.config("MYSQL_MAIN_TABLE") + " WHERE tgt_IMSI = " + str(particular_imsi)
        self.cursor2.execute(query)
        row_result = self.cursor2.fetchall()

        self.cursor2 = self.imsidb_custom.cursor(buffered=True)

        return row_result

    def custom_classify(self, imsi):
        # check if imsi is check_duplicate
        if(self.check_duplicate(imsi)):
            print("")
            print("DUPLICATE :" , imsi)
            print("")
            pass

        else:
            rows_imsi = self.get_imsi_details(imsi)
            # print(rows_imsi)

            #initialising the oldest first seen and latest last_seen
            oldest_datetime_f = rows_imsi[0][0]
            latest_datetime_l = rows_imsi[0][1]


            # Setting the time reference to compare to:-
            current_datetime = datetime.today()
            # before_1_week = current_datetime - timedelta(days = 7)
            # before_1_month = current_datetime - timedelta(days = 30)
            # before_6_month = current_datetime - timedelta(days = 183)

            # print(current_datetime)
            # print(before_1_week)
            # print(before_1_month)
            # print(before_6_month)

            #setting the oldest first seen and latest last_seen:-
            for i in rows_imsi:
                if (oldest_datetime_f > i[0]):
                    oldest_datetime_f = i[0]

            for i in rows_imsi:
                if (latest_datetime_l < i[1]):
                    latest_datetime_l = i[1]

            # print(oldest_datetime_f)
            # print(latest_datetime_l)
            # print("")
            # df = pd.DataFrame(rows_imsi)
            #
            # for x in rows_imsi:
                # print(x)
            # print(df)
            # ndf = df.sort_values(by = "")
            # print(df[0][0])
            # print(type(df[0][0]))

            # print(self.queries["query_1week_new"])

            ''' comparing the values to custom_classify the imsi in their respective columns '''
            string_colums = ""
            values = ""
            val_no = 0
            if (oldest_datetime_f >= self.fromdate and oldest_datetime_f <= self.todate ):
                string_colums = string_colums + "manual_new, "
                value = imsi + ","
                values = values + value
                val_no = val_no + 1

            if (latest_datetime_l >= self.fromdate and latest_datetime_l <= self.todate ):
                string_colums = string_colums + "manual_absent, "
                value = imsi + ","
                values = values + value
                val_no = val_no + 1


            string_colums = string_colums[:-2]
            values = values[:-1]
            # print(values)
            # print(string_colums)
            # print(val_no)
            # print("")

            query = "INSERT INTO imsiClassified_manualTable( {} ) VALUES ( {} )".format(string_colums, values)
            # print(query)
            self.cursor1.execute(query)
            self.imsidb_custom.commit()

            # for x in rows_imsi:
                # print(x)

            # print(datetime.today())
            # query = "SELECT tgt_IMSI, first_seen, last_seen FROM imsi"



    def main(self):
        from decouple import config
        self.config = config
        try:
            self.imsidb_custom = self.connector.connect(
                    host=config("MYSQL_HOST"),
                    user=config("MYSQL_USER"),
                    passwd=config("MYSQL_PASSWORD"),
                    database=config("MYSQL_DB")
                    )

        except self.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

        self.cursor1 = self.imsidb_custom.cursor(buffered=True)
        self.cursor2 = self.imsidb_custom.cursor(buffered=True)


        query = "CREATE TABLE IF NOT EXISTS imsiClassified_manualTable(  manual_absent TEXT, manual_new TEXT)"
        self.cursor1.execute(query)


        query = "SELECT tgt_IMSI FROM " + self.config("MYSQL_MAIN_TABLE")
        self.cursor1.execute(query)
        result_row = self.cursor1.fetchall()
        self.progress = 0
        length = len(result_row)
        for imsi in result_row:
            # print(str(imsi[0]))
            self.custom_classify(str(imsi[0]))
            self.progress = self.progress + 1
            self.percent = (self.progress/length)*100
            # print(int(self.percent), "%")
            self.signals.progress.emit(self.percent)

        self.signals.finished.emit()
        print('\ndone\n')



# if __name__ == "__main__":
#     import sys
#     fromdate = datetime(2020,7,16)
#     todate = datetime(2020,8,16)
#     print(fromdate)
#     print(todate)
#     print("")
#     cl = custom_classifier(connector, fromdate, todate)
#     cl.main()

    # app = QtWidgets.QApplication(sys.argv)
    # MainWindow = QtWidgets.QMainWindow()
    # myclass = ui_handle_progressbar(connector)
    # myclass.setupUi(MainWindow)
    # MainWindow.show()
    # myclass.start()
    # sys.exit(app.exec_())
