from flask import Flask, request, jsonify
from mysql import connector

import time, uuid

INT_MAX = 2147483647

app = Flask(__name__)

@app.route("/", methods=["GET"])
def wait_for_connection():
	return jsonify({"status": "OK"}), 200

@app.route("/route", methods=["GET"])
def get_optimal_route():
	# Se reseteaza conexiunea si obtine un cursor nou
	db.cmd_reset_connection()
	cursor = db.cursor()

	src = request.args.get("src")
	dst = request.args.get("dst")
	maxTrains = int(request.args.get("max_trains"))
	departureDay = int(request.args.get("departure_day"))

	open = []
	parents = {}

	lowest_time = INT_MAX
	optimal_route = []

	cursor.execute("select id, dst, hour, trip_time from trips where src = '{}' and day = {} and booked < total_seats * 11 / 10 and cancelled = false".format(src, departureDay))

	for (id, dest, hour, trip_time) in cursor.fetchall():
		departureTimestamp = departureDay * 24 + hour
		open.insert(0, ((dest, id), departureTimestamp + trip_time, departureTimestamp, 1))
		parents[(dest, id)] = (src, None)

	while len(open) > 0:
		# Extrag o stare
		(node, timestamp, departureTimestamp, trains) = open.pop(0)

		# Daca am ajuns la destinatie, se actualizeaza ruta optima si se continua cu o alta stare
		if node[0] == dst:
			travel_time = timestamp - departureTimestamp
			if travel_time < lowest_time:
				lowest_time = travel_time
				optimal_route = get_path(node, parents)
				continue

		# Daca am atins numarul maxim de trenuri, atunci continui cu o alta stare
		if trains >= maxTrains:
			continue

		cursor.execute("select id, dst, hour, day, trip_time from trips where src = '{}' and (day * 24 + hour) > {} and booked < total_seats * 11 / 10 and cancelled = false".format(node[0], timestamp))

		for (id, dest, hour, day, trip_time) in cursor.fetchall():
			if dest not in current_path(node, parents):
				open.insert(0, ((dest, id), day * 24 + hour + trip_time, departureTimestamp, trains + 1))
				parents[(dest, id)] = node

	if len(optimal_route) == 0:
		cursor.close()
		return jsonify({"status": []}), 200

	format_strings = ','.join(['{}'] * len(optimal_route))
	cursor.execute("select id, src, dst, hour, day, trip_time from trips where id in ({})".format(format_strings).format(*optimal_route))
	trips_info = cursor.fetchall()

	cursor.close()

	return jsonify({"status" : trips_info}), 200

def get_path(node, parents):
	result = []
	current = node
	while current[1] is not None:
		result.insert(0, current[1])
		current = parents[current]
	return result

def current_path(node, parents):
	result = set()
	current = node
	while current[1] is not None:
		result.add(current[0])
		current = parents[current]
	result.add(current[0])
	return result

@app.route("/book", methods=["GET"])
def book_ticket():
	# Se reseteaza conexiunea si obtine un cursor nou
	db.cmd_reset_connection()
	cursor = db.cursor()

	ids = request.args.getlist("trip_ids")

	format_strings = ','.join(['{}'] * len(ids))
	cursor.execute("select id, total_seats, booked, cancelled from trips where id in ({})".format(format_strings).format(*ids))
	available_trips = cursor.fetchall()

	for id in ids:
		found_trip = None
		for trip in available_trips:
			if id == trip[0]:
				found_trip = trip
				break

		# Verificam daca trenul exista
		if found_trip is None:
			cursor.close()
			return jsonify({"status" : "[ERROR] You can't book a ticket for this route because the trip " + id + " doesn't exist"}), 400

		(_, total_seats, booked, cancelled) = found_trip

		# Verificam daca trenul a fost anulat
		if cancelled:
			cursor.close()
			return jsonify({"status": "[ERROR] You can't book a ticket for this route because the trip " + id + " was cancelled"}), 400
		# Verificam daca poate fi rezervat un loc (se tine cont de politica de overbooking)
		if booked >= total_seats * 11 / 10:
			cursor.close()
			return jsonify({"status": "[ERROR] You can't book a ticket for this route because all seats of the trip " + id + " were booked"}), 400

	bookingId = str(uuid.uuid4())

	for id in ids:
		cursor.execute("update trips set booked = booked + 1 where id = '{}'".format(id))
		cursor.execute("insert into reservations values ('{}', '{}')".format(bookingId, id))

	cursor.execute("insert into bookingIds (bookingId) values ('{}')".format(bookingId))

	cursor.close()
	db.commit()

	return jsonify({"status": "Route has been booked: " + bookingId}), 200

@app.route("/buy", methods=["GET"])
def buy_ticket():
	# Se reseteaza conexiunea si obtine un cursor nou
	db.cmd_reset_connection()
	cursor = db.cursor()

	bookingId = request.args.get("booking_id")
	creditCard = request.args.get("credit_card_info")

	cursor.execute("select available from bookingIds where bookingId = '{}'".format(bookingId))
	availables = cursor.fetchall()
	if len(availables) == 0:
		cursor.close()
		return jsonify({"status": "[ERROR] This bookingId doesn't exist\n"}), 400
	if not availables[0][0]:
		cursor.close()
		return jsonify({"status": "[ERROR] You have previously bought tickets for this bookingId\n"}), 400

	cursor.execute("select tripId from reservations where bookingId = '{}'".format(bookingId))

	ids = []
	for id in cursor.fetchall():
		ids.append(id[0])

	format_strings = ','.join(['{}'] * len(ids))
	cursor.execute("select id, src, dst, hour, day, trip_time, total_seats, bought, cancelled from trips where id in ({})".format(format_strings).format(*ids))

	boarding_pass = {}

	for (id, src, dst, hour, day, trip_time, total_seats, bought, cancelled) in cursor.fetchall():
		# Verificam daca trenul a fost intre timp anulat
		if cancelled:
			cursor.close()
			return jsonify({"status": "[ERROR] You can't buy the tickets of this reservation because the trip " + id + " was cancelled"}), 400

		boarding_pass[id] = (src, dst, hour, day, trip_time)
		if bought >= total_seats:
			cursor.close()
			return jsonify({"status": "[ERROR] You can't buy the tickets of this reservation because all seats of the trip " + id + " were sold."}), 400

	message = "Boarding Pass:\n"

	for id in sorted(ids, key = lambda id : boarding_pass[id][3] * 24 + boarding_pass[id][2]):
		(src, dst, hour, day, trip_time) = boarding_pass[id]
		cursor.execute("update trips set bought = bought + 1 where id = '{}'".format(id))
		message = message + "Trip " + id + " from " + src + " to " + dst + " departs at hour " + str(hour) + ", day " + str(day) + " and will take " + str(trip_time) + " hours\n"

	cursor.execute("update bookingIds set available = false where bookingId = '{}'".format(bookingId))

	cursor.close()
	db.commit()

	return jsonify({"status": message}), 200

if __name__ == "__main__":
	# Conectarea la baza de date
	while True:
		try:
			db = connector.MySQLConnection(
				host="mysql",
				database="trains",
				user="admin",
				passwd="admin")
			break
		except:
			time.sleep(1)

	app.run(host="0.0.0.0", port=20000)
