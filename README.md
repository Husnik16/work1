КОД БОТА 

import pyodbc
import telebot
import logging
from io import BytesIO
import pandas as pd
from telebot import types

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bot_log.txt"),
        logging.StreamHandler()
    ]
)


def main():

    API_TOKEN = '____'


    # Задаем список кнопок
    buttons = ["1", "2", "3", "4", "5", "6"]

    bot = telebot.TeleBot(API_TOKEN)

    # Handle '/start' and '/help'
    @bot.message_handler(func=lambda message: True)
    def get_agency(message):
        user = message.from_user
        user_name = f"{user.first_name} {user.last_name}" if user.last_name else user.first_name
        logging.info(f"Получено сообщение: {message.text} от пользователя {user_name}")
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        for button in buttons:
            markup.add(types.KeyboardButton(button))
        bot.send_message(message.chat.id, 'Выберите агентство', reply_markup=markup)
        bot.register_next_step_handler(message, get_type)



    def get_type(message):
        global agency
        agency = message.text
        user = message.from_user
        if agency in buttons:
            user_name = f"{user.first_name} {user.last_name}" if user.last_name else user.first_name
            logging.info(f"Получено сообщение: {message.text} от пользователя {user_name}")
            markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
            que1 = types.KeyboardButton("Проверка PICoS")
            que2 = types.KeyboardButton("Проверка матрицы ТТ")
            markup.add(que1, que2)
            bot.send_message(message.chat.id, 'Что необходимо проверить?', reply_markup=markup)
            bot.register_next_step_handler(message, chooseOperation)

        else:
            reply_markup = types.ReplyKeyboardRemove()
            bot.send_message(message.chat.id, 'Некорректный ввод, пожалуйста, повторите операцию', reply_markup=reply_markup)

    def chooseOperation(message):
        global operType
        operType = message.text
        user = message.from_user
        reply_markup = types.ReplyKeyboardRemove()
        if operType == "Проверка PICoS":

            user_name = f"{user.first_name} {user.last_name}" if user.last_name else user.first_name
            logging.info(f"Получено сообщение: {message.text} от пользователя {user_name}")
            bot.send_message(message.from_user.id, 'Уточните ФИО сотрудника,sap код ТТ,дату визита.\n'
                                                   'Формат: Иванов, 850186548, 2024-12-31', reply_markup=reply_markup)
            bot.register_next_step_handler(message, getPicos)  # следующий шаг – функция get_surname
        elif operType == "Проверка матрицы ТТ":
            user_name = f"{user.first_name} {user.last_name}" if user.last_name else user.first_name
            logging.info(f"Получено сообщение: {message.text} от пользователя {user_name}")
            bot.send_message(message.from_user.id, 'Уточните sap код ТТ\n'
                                                   'Формат: 850186548', reply_markup=reply_markup)
            bot.register_next_step_handler(message, getTTMatrix)

    def getPicos(message):
        global arrData
        arrData = message.text
        user = message.from_user
        user_name = f"{user.first_name} {user.last_name}" if user.last_name else user.first_name
        logging.info(f"Получено сообщение: {message.text} от пользователя {user_name}")
        bot.reply_to(message, "Ок ищу данные!")
        try:
            (FIO, SapCode, dateS) = str.split(arrData, ',')
            arr = GetPICOSData(SapCode, FIO, dateS, agency)
            xlDoc = sendXlReport(arr)
            bot.send_document(message.chat.id, xlDoc, visible_file_name=f'Анализ PICoS {FIO},ТТ {SapCode},{dateS}.xlsx')
        except Exception as e:
            logging.error(f"Ошибка при проверке PICoS: {e}")
            bot.send_message(message.chat.id, 'Произошла ошибка при обработке данных.')


    def getTTMatrix(message):
        global sapData
        sapData = message.text
        user = message.from_user
        user_name = f"{user.first_name} {user.last_name}" if user.last_name else user.first_name
        logging.info(f"Получено сообщение: {message.text} от пользователя {user_name}")
        bot.reply_to(message, "Ищу данные")
        try:
            arr = getMatrixData(sapData, agency)
            xlDoc = sendXlReport(arr)
            bot.send_document(message.chat.id, xlDoc, visible_file_name=f'Матрица ТТ {sapData}.xlsx')
        except Exception as e:
            logging.error(f"Ошибка при проверке матрицы ТТ: {e}")
            bot.send_message(message.chat.id, 'Произошла ошибка при обработке данных.')

    bot.infinity_polling()


def connectDB(agency):
    # Параметры подключения
    server = "--------"
    database = ""
    if agency == '1':
        database = '***'

    if agency == '2':
        database = '***'

    if agency == '3':
        database = '***'

    if agency == '4':
        database = '***'

    if agency == '5':
        database = '***'

    if agency == '6':
        database = '***'

    username = '*****'
    password = '*****'

    # Строка подключения
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

    # Подключение к базе данных
    conn = pyodbc.connect(connection_string)

    return conn


def GetPICOSData(tt, fName, dateS, agency):
    conn = connectDB(agency)
    # Создание курсора для выполнения SQL-запросов
    cursor = conn.cursor()

    # Пример выполнения SQL-запроса
    cursor.execute(
        f"""SELECT  
            AttrValueName
          ,[Value]
          ,[MaxValue]
          ,nodeNumber

      FROM [dbo].[DS_PerfectStore_CalculatedValues] dpc with (nolock)
      join DS_AttributesValues dsa with (nolock)
      on dsa.AttrID = dpc.AttrID and dsa.AttrValueID = dpc.AttrValueID
      join ds_faces df with (nolock) 
      on df.fid = dpc.masterfid
       join ds_faces df2 with (nolock) 
      on df2.fid = dpc.mfid
      where 
      --ид сотрудника
      df.fname like '%{str.strip(fName)}%' and 
      --ид точки
      df2.exid = '{str.strip(tt)}' and 
      --дата за которые нужны баллы
      ValueDate = '{str.strip(dateS)}' 
      -- показатели касающиеся пикос
     and dpc.AttrValueID in(190852,111017,50021,230118,50022,50015,111745,50020,50025,50024,
     50027,50019,804907,50012,341103,870473,999956,693557,50053)
     and Dept in (2,3)
     --номер фотоаудита
     and NodeNumber in (1,2)
    order by AttrValueName, NodeNumber
     """)

    # Получение и вывод результатов
    rows = cursor.fetchall()
    result_list = [
        list([str(row[0]), round(float(row[1]), 2), round(float(row[2]), 2), int(row[3])])
        for row in rows
    ]

    # Закрытие соединения
    conn.close()

    raw = pd.DataFrame(result_list, columns=['AttrValueName', 'Values', 'MaxValue', 'NodeNumber'])

    raw.columns = ['AttrValueName', 'Values', 'MaxValue', 'NodeNumber']
    pivoted_data = raw.pivot_table(
        index=['AttrValueName', 'MaxValue'],
        columns='NodeNumber',
        values='Values',
        aggfunc='first'
    ).reset_index()
    return pivoted_data


def getMatrixData(tt, agency):
    # Подключение к базе данных
    conn = connectDB(agency)

    # Создание курсора для выполнения SQL-запросов
    cursor = conn.cursor()

    # Пример выполнения SQL-запроса
    cursor.execute(
        f"""select  di.iidText, di.iName, doa.Changedate
        from DS_ITEMS di
        join DS_ObjectsAttributes doa
        on di.iid = doa.id
        where dictid = 1
        and di.activeFlag = 1
        and doa.Activeflag = 1
        and doa.AttrId = 611
        and doa.AttrValueId in
          (
            
            select doa.AttrValueId
            from DS_ObjectsAttributes doa
            join ds_faces df
            on df.fid = doa.id
            where df.exid in 
            
            (
                '{str.strip(tt)}'
            )
            and doa.DictId = 2
            and doa.Activeflag =1 
            and doa.AttrId = 611
        )""")

    # Получение и вывод результатов
    rows = cursor.fetchall()
    result_list = [
        list(row) for row in rows
    ]

    # Закрытие соединения
    conn.close()

    raw = pd.DataFrame(result_list, columns=['iidText', 'iName', 'Changedate'])

    raw.columns = ['iidText', 'iName', 'Changedate']

    return raw


def sendXlReport(data):
    excel_file = BytesIO()
    writer = pd.ExcelWriter(excel_file, engine='xlsxwriter')
    data.to_excel(writer, sheet_name='Data', index=False)
    writer._save()

    excel_file.seek(0)
    return excel_file


main()
