from datetime import datetime
dt_string = "7/9/2017 12:00:00 am"

dt_object1 = datetime.strptime(dt_string, "%d/%m/%Y %H:%M:%S %p")
print("dt_object1:", dt_object1)


def convert_date(date):
    return datetime.strptime(date, "%d/%m/%Y %H:%M:%S %p")

if __name__ == '__main__':
    print(convert_date(dt_string))