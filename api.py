from fastapi import FastAPI, Query, Body
from fastapi.responses import RedirectResponse
import sqlite3
from run_api import check_success

database = sqlite3.connect("bondit.db")
app = FastAPI()


def to_json_data(flights):
    new_data = []
    for flight in flights:
        new_data.append({'flight_id': flight[0], 'arrival': flight[1], 'departure': flight[2], 'success': flight[3]})
    return new_data


@app.get("/")
def redirect_root():
    return RedirectResponse("/docs")


@app.get("/flight")
async def get_flight_data(flight_id: str = Query(default=None, alias='flightId')):
    data = database.execute("select * from flights where flight_id = ? order by arrival", (flight_id, ))
    flight_data = data.fetchall()
    return to_json_data(flight_data)


@app.post("/flight")
async def add_flight_data(flight_id: str = Body(...),
                          arrival: str = Body(...),
                          departure: str = Body(...)):
    data = database.execute("select * from flights where flight_id = ? order by arrival", (flight_id,))
    flight_data = data.fetchall()
    count_success = database.execute("select * from success_count").fetchall()[0][0]
    status = check_success(count_success, arrival, departure, flight_data)
    database.execute("INSERT INTO flights ('flight_id', 'arrival', 'departure', 'success') VALUES (?, ?, ?, ?)",
                     (flight_id, arrival, departure, status))
    if status == 'success':
        count_success += 1
        database.execute("UPDATE success_count SET count_success = ?", (count_success,))
    database.commit()
    return {'message': f'flight {flight_id} added to data. the status is {status}'}
