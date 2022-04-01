import pyodbc #воспользуемся заранее установленной библиотекой (не системной) для взаимодействия с MS SQL Server

class Sql: #создадим класс для работы с сервером и базой данных
    def __init__(self, database, server): #создадим функцию для работы с сервером и базой данных
        countryname = [] #заранее обозначим массив названий стран, которые позже туда занесём
        cityname = [] #также массив для названий городов
        countryid = [] #для id стран, взятых из таблицы Cities
        countryId = [] #для id стран, взятых из таблицы Countries
        cityId = [] #для id городов, взятых из таблицы Cities
        cityid = [] #для id городов, взятых из таблицы Companies
        result = [] #для удобства позже занесём в данный массив все необходимые для новой таблицы данные
        
        connection = pyodbc.connect("Driver={SQL Server Native Client 11.0};" #создадим переманную, содержащую необходимые данные для соединения с сервером и базой данных
                                   "Server="+server+";"
                                   "Database="+database+";"
                                   "Trusted_Connection=yes;", autocommit = True)
        companiesReq = 'select id, name, city_id, revenue, labors from Companies' #переменная, содержащая в себе запрос для вывода информации из таблицы Companies
        citiesReq = 'select id, name, population, founded, country_id from Cities' #переменная, содержащая в себе запрос для вывода информации из таблицы Cities
        countriesReq = 'select id, name, population, gdp from Countries' #переменная, содержащая в себе запрос для вывода информации из таблицы Countries
        companiesCursor = connection.cursor() #курсор для таблицы Companies
        companiesCursor.execute(companiesReq) #отправлеие запроса в базу данных
        for row in companiesCursor: #цикл для возможности проверки каждой компании на количество сотрудников и вывода данных
            if row.labors > 999: #условие (количество сотрудников не менее 1000)
                cityid.append(row.city_id) #сохранение city_id той компании, которая подходит нам по условию
        citiesCursor = connection.cursor() #курсор для таблицы Cities
        citiesCursor.execute(citiesReq) #отправлеие запроса в базу данных
        for row in citiesCursor: #цикл, созданный для возможности вывода данных всех городов, в которых могут находиться рассматриваемые компании
            cityId.append(row.id) #добавляем id каждого города в массив cityId
            cityname.append(row.name) #добавляем имя каждого города в массив cityname
            countryid.append(row.country_id) #добавляем country_id каждого города в массив countryid
        countriesCursor = connection.cursor() #курсор для таблицы Countries
        countriesCursor.execute(countriesReq) #отправлеие запроса в базу данных
        for row in countriesCursor: #цикл, созданный для возможности вывода данных всех стран, в которых могут находиться рассматриваемые компании
            countryname.append(row.name) #добавляем имя каждой страны в массив countryname
            countryId.append(row.id) #добавляем id каждой страны в массив countryId
        connection.commit()
        for i in range(len(countryname)): #для начала создадим цикл для занесения названий каждой страны и счётчиков подходящих компаний, идущих каждый - после названия своей страны
            result.append(countryname[i])
            result.append(0) #изначально каждый счётчик равен нулю
        for i in range(len(cityid)): #теперь воспользуемся циклами, которые помогут вычислить верно количество подходящих компаний в каждую страну. при каждом совпадении счётчик будет увеличиваться на 1
            for j in range(len(countryid)):
                for k in range(len(countryId)):
                    if cityid[i] == cityId[j]:
                        if countryid[j] == countryId[k]:
                            if k > 0:
                                result[k*2+1] += 1
                            else:
                                result[1] += 1
        
        createRequest = 'create table Big_companies (country varchar(20), \
        amount_of_big_companies int)'
        createCursor = connection.cursor()
        createCursor.execute(createRequest) #аналогично прошлому запросу, напишем ещё один, создающий новую таблицу, в которую мы позже занесём наши данные.
        for i in range(len(countryname)): #создадим цикл, где каждая итерация соответствует внесению одной страны и числа подходящих компаний
            insertRequest = f"insert into Big_companies values('{result[i*2]}', \
            {result[i*2+1]})"
            insertCursor = connection.cursor()
            insertCursor.execute(insertRequest) #соответствующие запросы, отправляемые в базу данных 1 раз за каждую итерацию цикла, где количество итераций равно количеству рассматриваемых стран