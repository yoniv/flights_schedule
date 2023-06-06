import uvicorn
import csv
from datetime import datetime
import sqlite3

database = sqlite3.connect("bondit.db")


def create_table():
    database.execute("create table if not exists flights (flight_id TEXT, arrival timestamp, departure timestamp, success TEXT)")
    database.execute("create table if not exists success_count (count_success INTEGER)")
    database.execute("delete from flights")
    database.execute("delete from success_count")
    database.execute("INSERT INTO success_count ('count_success') VALUES (?)", (0,))
    database.commit()


def check_success(count_success, arrive_str, departure_str, flights_schedule=None):
    arrive = datetime.strptime(arrive_str, '%H:%M')
    departure = datetime.strptime(departure_str, '%H:%M')
    diff = departure - arrive
    if diff.seconds // 60 < 180 or count_success >= 20:
        return 'fail'
    if flights_schedule:
        return check_flight_schedule(flights_schedule, arrive, departure)
    return 'success'


def check_flight_schedule(flights_schedule, arrive, departure):
    for flight in flights_schedule:
        if flight[3] != 'success':
            continue
        success_arrive = datetime.strptime(flight[1], '%H:%M')
        success_departure = datetime.strptime(flight[2], '%H:%M')
        if success_arrive <= arrive <= success_departure or success_arrive <= departure <= success_departure:
            return 'fail'
    return 'success'


def csv_to_db():
    headers_line = True
    count_success = 0
    with open("data.csv", "r") as file:
        csv_reader = csv.reader(file)
        for line in csv_reader:
            if headers_line:
                headers_line = False
                continue
            flights_data = database.execute("SELECT * FROM flights WHERE flight_id = ? order by arrival", (line[0],))
            flights = flights_data.fetchall()
            if flights and count_success < 20:
                success_field = check_success(count_success, line[1], line[2], flights)
            elif count_success < 20:
                success_field = check_success(count_success, line[1], line[2])
            database.execute("INSERT INTO flights ('flight_id', 'arrival', 'departure', 'success') VALUES (?, ?, ?, ?)",
                             (line[0], line[1], line[2], success_field))
            database.commit()

            count_success += 1 if success_field == 'success' else 0
        database.execute("UPDATE success_count SET count_success = ?", (count_success,))
        database.commit()


if __name__ == '__main__':
    create_table()
    csv_to_db()
    uvicorn.run("api:app",
                host="0.0.0.0",
                port=8000)
