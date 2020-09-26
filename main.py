#!/usr/bin/python3
import GUI, progress_information_table
import classifier
import custom_classifier
from PyQt5 import QtCore, QtGui, QtWidgets
# from PyQt5.QtCore import Qt
from PyQt5.QtGui import *
from PyQt5.QtCore import *
import pandas as pd
import mysql.connector as connector
import sys
from datetime import datetime

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


class pandasModel(QtCore.QAbstractTableModel):

    def __init__(self, data):
        QtCore.QAbstractTableModel.__init__(self)
        self._data = data

    def rowCount(self, parent=None):
        return self._data.shape[0]

    def columnCount(self, parnet=None):
        return self._data.shape[1]

    def data(self, index, role=Qt.DisplayRole):
        if index.isValid():
            if role == Qt.DisplayRole:
                return str(self._data.iloc[index.row(), index.column()])
        return None

    @QtCore.pyqtSlot(int, QtCore.Qt.Orientation, result=str)
    def headerData(self, col, orientation: QtCore.Qt.Orientation, role):
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
            # print(self._data.columns[col])
                list = ["isensor_no", "tgt_IMSI", "tgt_TA", "icountry", "inetwork", "ioperator", "btc_mcc", "btc_mnc", "btc_lac", "btc_cellid", "btc_arfcn", "count", "first_seen", "last_seen"]
                return list[col]

            else:
                return str(self._data.index[col])

        return QtCore.QVariant()


class UI_handler(GUI.Ui_MainWindow, QtWidgets.QMainWindow, QtWidgets.QDialog):
    def __init__(self, connector):
        super(UI_handler, self).__init__()
        self.connector = connector
        self.initialiser()

        self.get_lengths()
        # print(self.dictionary_count)


        # self.workThread = classify_start(self.connector)

    def onswitch(self):
        radiobutton = self.sender()
        if radiobutton.isChecked():
            # print(radiobutton.val)
            if radiobutton.val == "relative":
                month_absent_relative = int(self.dictionary_count["month_absent"]) - int(self.dictionary_count["6month_absent"])
                week_absent_relative = int(self.dictionary_count["week_absent"]) - int(self.dictionary_count["month_absent"])
                month_new_relative = int(self.dictionary_count["month_new"]) - int(self.dictionary_count["week_new"])
                sixmonth_new_relative = int(self.dictionary_count["6month_new"]) - int(self.dictionary_count["month_new"])

                #setting the values to text
                self.week_absent.setText(str(week_absent_relative))
                self.month_absent.setText(str(month_absent_relative))
                self.month_new.setText(str(month_new_relative))
                self.sixmonth_new.setText(str(sixmonth_new_relative))
            else:
                #setting the values to text
                self.week_absent.setText(self.dictionary_count["week_absent"])
                self.week_new.setText(self.dictionary_count["week_new"])
                self.month_absent.setText(self.dictionary_count["month_absent"])
                self.month_new.setText(self.dictionary_count["month_new"])
                self.sixmonth_absent.setText(self.dictionary_count["6month_absent"])
                self.sixmonth_new.setText(self.dictionary_count["6month_new"])



    def buttons(self):
        # setting the values count to their buttons:
        self.week_absent.setText(self.dictionary_count["week_absent"])
        self.week_new.setText(self.dictionary_count["week_new"])
        self.month_absent.setText(self.dictionary_count["month_absent"])
        self.month_new.setText(self.dictionary_count["month_new"])
        self.sixmonth_absent.setText(self.dictionary_count["6month_absent"])
        self.sixmonth_new.setText(self.dictionary_count["6month_new"])

        #for IMSI tab, refresh button
        self.refresh_tableview.clicked.connect(lambda: self.tableView_update())

        #to set function of uit button in file menu
        self.actionQuit.triggered.connect(sys.exit)

        # code for update button:
        self.update.clicked.connect(self.run_classifier)
        # self.update.clicked.connect(self.workThread.start)

        # for radio buttons
        self.radio_absolute.val = "absolute"
        self.radio_absolute.toggled.connect(self.onswitch)
        self.radio_relative.val = "relative"
        self.radio_relative.toggled.connect(self.onswitch)

        #for information buttons:
        self.week_absent.clicked.connect(lambda: self.imsi_list_by_categry("week_absent", "imsiClassified"))
        self.week_new.clicked.connect(lambda: self.imsi_list_by_categry("week_new", "imsiClassified"))
        self.month_absent.clicked.connect(lambda: self.imsi_list_by_categry("month_absent", "imsiClassified"))
        self.month_new.clicked.connect(lambda: self.imsi_list_by_categry("month_new", "imsiClassified"))
        self.sixmonth_absent.clicked.connect(lambda: self.imsi_list_by_categry("6month_absent", "imsiClassified"))
        self.sixmonth_new.clicked.connect(lambda: self.imsi_list_by_categry("6month_new", "imsiClassified"))

            #for custom information buttons:-
        self.custom_absent.clicked.connect(lambda: self.imsi_list_by_categry("manual_absent", "imsiClassified_manualTable"))
        self.custom_new.clicked.connect(lambda: self.imsi_list_by_categry("manual_new", "imsiClassified_manualTable"))


        #date edit changes:
        self.fromdate.dateChanged.connect(self.onDateChanged_from)
        self.todate.dateChanged.connect(self.onDateChanged_to)

        # get button role:
        self.custom_get.clicked.connect(self.custom_get_mathod)

    def custom_get_mathod(self):
        self.custom_get.setText("WAIT!")
        query = "DROP TABLE IF EXISTS imsiClassified_manualTable"
        self.cursor.execute(query)
        self.imsidb.commit()

        self.workthread_custom = custom_get_methodclass(self.connector, self.from_date, self.to_date)
        self.workthread_custom.start()
        self.workthread_custom.signals.progress.connect(self.set_get_button_value)
        self.workthread_custom.signals.finished.connect(self.custom_process_finished)


    def set_get_button_value(self,n):
        self.custom_get.setText(str(n) + "%")


    def custom_process_finished(self):
        print("Custom table updated...!!")
        self.custom_get.setText("DONE")
        self.update_custom_dictionary()

        self.custom_new.setText(self.dictionary_count["manual_new"])
        self.custom_absent.setText(self.dictionary_count["manual_absent"])

    def onDateChanged_from(self, qDate_from):
        print('{0}/{1}/{2}'.format(qDate_from.day(), qDate_from.month(), qDate_from.year()))
        self.from_date = datetime(qDate_from.year(),qDate_from.month(),qDate_from.day())
        # print(self.from_date)


    def onDateChanged_to(self, qDate_to):
        print('{0}/{1}/{2}'.format(qDate_to.day(), qDate_to.month(), qDate_to.year()))
        self.to_date = datetime(qDate_to.year(),qDate_to.month(),qDate_to.day())


    def run_classifier(self):
        import sys

        query = "DROP TABLE IF EXISTS imsiClassified"
        self.cursor.execute(query)
        self.imsidb.commit()

        app2 = QtWidgets.QApplication(sys.argv)
        MainWindow2 = QtWidgets.QMainWindow()
        myclass_classifier = classifier.ui_handle_progressbar(self.connector)
        myclass_classifier.setupUi(MainWindow2)
        MainWindow2.show()
        myclass_classifier.start()


    def get_lengths(self):
        self.dictionary_count = {}

        list = ["week_absent", "month_absent", "6month_absent", "week_new", "month_new", "6month_new"]
        for i in list:
            length = self.get_length_column(i)
            self.dictionary_count[i] = str(length)

    def update_custom_dictionary(self):
        list_custom = ["manual_new", "manual_absent"]
        for j in list_custom:
            length = self.get_length_column_custom(j)
            self.dictionary_count[j] = str(length)


    def get_length_column_custom(self, column_custom):
        query = "SELECT " + column_custom + " FROM imsiClassified_manualTable"
        # print(query)
        self.cursor.execute(query)
        whole_list = self.cursor.fetchall()
        # print(whole_list)
        temp_length = 0
        for i in whole_list:
            if i[0] == None :
                continue
            else:
                temp_length = temp_length + 1
        # print(temp_length)
        return temp_length


    def get_length_column(self, column_name):
        try:
            query = "SELECT " + column_name + " FROM imsiClassified"
            # print(query)
            self.cursor.execute(query)
            whole_list = self.cursor.fetchall()
            # print(whole_list)
            temp_length = 0
            for i in whole_list:
                # print(i)
                if i[0] == None:
                    continue
                else:
                    temp_length = temp_length + 1
            # print(temp_length)
            return temp_length

        except:
            query = "CREATE TABLE IF NOT EXISTS imsiClassified( week_absent TEXT, month_absent TEXT, 6month_absent TEXT, manual_absent TEXT, week_new TEXT, month_new TEXT, 6month_new TEXT, manual_new TEXT)"
            self.cursor.execute(query)
            return 0




    def tableView_update(self):
        self.cursor.execute("SELECT * FROM " + self.config("MYSQL_MAIN_TABLE"))
        d = self.cursor.fetchall()
        df = pd.DataFrame(d)
        self.model = pandasModel(df)
        self.tableView.setModel(self.model)
        self.tableView.normalGeometry()


    def initialiser(self):
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


        self.cursor = self.imsidb.cursor(buffered=True)



    def imsi_list_by_categry(self,categry, table):
        self.listWidget.clear()
        query = "SELECT " + categry + " FROM " + table
        self.cursor.execute(query)
        self.label_catagorie.setText(categry)
        imsi_list = self.cursor.fetchall()
        print(imsi_list)
        for i in imsi_list:
            self.listWidget.addItem(i[0])


class custom_get_methodclass(QThread, QRunnable, custom_classifier.custom_classifier):

    def __init__(self,connector, fromdate, todate):
        super(custom_get_methodclass, self).__init__()
        self.connector = connector
        self.fromdate = fromdate
        self.todate = todate

        self.signals = WorkerSignals()

    @pyqtSlot()
    def run(self):
        self.main()



if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    MainWindow = QtWidgets.QMainWindow()
    ui = UI_handler(connector)
    ui.setupUi(MainWindow)
    MainWindow.show()
    ui.buttons()
    sys.exit(app.exec_())
