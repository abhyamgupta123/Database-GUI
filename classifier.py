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



class WorkerSignals(QObject):
    '''
    Defines the signals available from a running worker thread.

    Supported signals are:

    finished
        No data

    error
        `tuple` (exctype, value, traceback.format_exc() )

    result
        `object` data returned from processing, anything

    progress
        `int` indicating % progress

    '''
    finished = pyqtSignal()
    error = pyqtSignal(tuple)
    result = pyqtSignal(object)
    progress = pyqtSignal(int)


class ui_handle_progressbar(progress_information_table.Ui_MainWindow, QtWidgets.QMainWindow, QtWidgets.QDialog):
    def __init__(self, connector):
        super(ui_handle_progressbar, self).__init__()
        self.connector = connector

        self.workThread = report(self.connector)

    def update_progressbar(self, n):
        self.progressBar.setValue(n)

    def start(self):
        self.workThread.start()
        self.workThread.signals.progress.connect(self.update_progressbar)
        self.quitupdating.clicked.connect(QtCore.QCoreApplication.instance().quit)
        self.Quitbutton.triggered.connect(sys.exit)



class report(QThread, QRunnable):
    def __init__(self, connector):
        super(report, self).__init__()
        self.connector = connector

        self.signals = WorkerSignals()

        self.queries = {"query_1week_new": "week_new, ",
                        "query_1month_new": "month_new, ",
                        "query_6month_new": "6month_new, ",
                        "query_1week_absent": "week_absent, ",
                        "query_1month_absent": "month_absent, ",
                        "query_6month_absent": "6month_absent, "
                        }
        # self.main()


    @pyqtSlot()
    def run(self):
        self.main()


    def check_duplicate(self, imsi_no):
        query = "SELECT * FROM imsiClassified "
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

        self.cursor2 = self.imsidb.cursor(buffered=True)

        return row_result

    def classify(self, imsi):
        # check if imsi is check_duplicate
        if(self.check_duplicate(imsi)):
            print("")
            print("DUPLICATE " , imsi)
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
            before_1_week = current_datetime - timedelta(days = 7)
            before_1_month = current_datetime - timedelta(days = 30)
            before_6_month = current_datetime - timedelta(days = 183)

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
            # df = pd.DataFrame(rows_imsi)
            #
            # for x in rows_imsi:
                # print(x)
            # print(df)
            # ndf = df.sort_values(by = "")
            # print(df[0][0])
            # print(type(df[0][0]))

            # print(self.queries["query_1week_new"])

            ''' comparing the values to classify the imsi in their respective columns '''
            string_colums = ""
            values = ""
            val_no = 0
            if oldest_datetime_f >= before_1_week:
                value = imsi + ","
                values = values + value
                temp_query = self.queries["query_1week_new"]

                string_colums = string_colums + temp_query
                val_no = val_no + 1



            if oldest_datetime_f >= before_1_month:
                value = imsi + ","
                values = values + value
                temp_query = self.queries["query_1month_new"]


                string_colums = string_colums + temp_query
                val_no = val_no + 1



            if oldest_datetime_f >= before_6_month:
                value = imsi + ","
                values = values + value
                temp_query = self.queries["query_6month_new"]


                string_colums = string_colums + temp_query
                val_no = val_no + 1



            if latest_datetime_l <= before_1_week:
                value = imsi + ","
                values = values + value
                temp_query = self.queries["query_1week_absent"]


                string_colums = string_colums + temp_query
                val_no = val_no + 1



            if latest_datetime_l <= before_1_month:
                value = imsi + ","
                values = values + value
                temp_query = self.queries["query_1month_absent"]


                string_colums = string_colums + temp_query
                val_no = val_no + 1



            if latest_datetime_l <= before_6_month:
                value = imsi + ","
                values = values + value
                temp_query = self.queries["query_6month_absent"]


                string_colums = string_colums + temp_query
                val_no = val_no + 1


            string_colums = string_colums[:-2]
            values = values[:-1]
            # print(values)
            # print(string_colums)
            # print(val_no)
            # print("")

            query = "INSERT INTO imsiClassified( {} ) VALUES ( {} )".format(string_colums, values)
            # print(query)
            self.cursor1.execute(query)
            self.imsidb.commit()

            # for x in rows_imsi:
                # print(x)

            # print(datetime.today())
            # query = "SELECT tgt_IMSI, first_seen, last_seen FROM imsi"



    def main(self):
        from decouple import config
        self.config = config
        try:
            self.imsidb = self.connector.connect(
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

        self.cursor1 = self.imsidb.cursor(buffered=True)
        self.cursor2 = self.imsidb.cursor(buffered=True)


        query = "CREATE TABLE IF NOT EXISTS imsiClassified( week_absent TEXT, month_absent TEXT, 6month_absent TEXT, manual_absent TEXT, week_new TEXT, month_new TEXT, 6month_new TEXT, manual_new TEXT)"
        self.cursor1.execute(query)


        query = "SELECT tgt_IMSI FROM " + self.config("MYSQL_MAIN_TABLE")
        self.cursor1.execute(query)
        result_row = self.cursor1.fetchall()
        progress = 1
        length = len(result_row)
        for imsi in result_row:
            # print(str(imsi[0]))
            self.classify(str(imsi[0]))
            progress = progress + 1
            percent = (progress/length)*100
            # print(percent)
            self.signals.progress.emit(percent)

        print('\ndone\n')


#
# if __name__ == "__main__":
#     import sys
#     app = QtWidgets.QApplication(sys.argv)
#     MainWindow = QtWidgets.QMainWindow()
#     myclass = ui_handle_progressbar(connector)
#     myclass.setupUi(MainWindow)
#     MainWindow.show()
#     myclass.start()
#     sys.exit(app.exec_())
#
#     myclass.classify('404545281787025')
