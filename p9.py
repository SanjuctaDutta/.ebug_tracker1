import csv 

from flask import Flask, render_template, redirect, url_for, request,jsonify
import flask
from flask import Flask, render_template, redirect, url_for, flash, abort
from flask_bootstrap import Bootstrap
from datetime import date
from functools import wraps

from requests import Session
from sqlalchemy import ForeignKey
from werkzeug.security import generate_password_hash, check_password_hash
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import relationship
from mftool import Mftool
import json
import pandas as pd
from datetime import datetime
mf= Mftool()
print(mf)
from flask_login import UserMixin, login_user, LoginManager, login_required, current_user, logout_user
app = Flask(__name__, template_folder="template")
app.app_context().push()
app.config['SECRET_KEY'] = '8BYkEfBA6O6donzWlSihBXox7C0sKR6b'
Bootstrap(app)

app.config['SQLALCHEMY_DATABASE_URI']='postgresql://postgres:helloworld@localhost/test11db'
db= SQLAlchemy(app)

class AMCs_list(db.Model):
    __tablename__ = "AMCs_list"
    amc_id = db.Column(db.Integer, primary_key=True)
    amc_name = db.Column(db.String(5000))
    m1 = relationship("schemes_list", back_populates="p1")

db.create_all()


class schemes_list(db.Model):
    __tablename__ = "schemes_list"
    amc_id = db.Column(db.Integer, db.ForeignKey("AMCs_list.amc_id"))
    p1 = relationship("AMCs_list", back_populates="m1")
    scheme_id = db.Column(db.Integer, primary_key=True)
    scheme_name = db.Column(db.String(5000))
    direct_code = db.Column(db.Integer, unique=True)
    d1 = relationship("direct_nav", backref="scheme")
    regular_code = db.Column(db.Integer, unique=True)
    r1 = relationship("regular_nav", backref="scheme")

db.create_all()


class direct_nav(db.Model):
    __tablename__ = "direct_nav"
    id = db.Column(db.Integer, primary_key=True)
    direct_code = db.Column(db.Integer, db.ForeignKey("schemes_list.direct_code"))
    nav_data_date = db.Column(db.String(50))
    direct_nav_double_precision = db.Column(db.Numeric(10, 2))

db.create_all()


class regular_nav(db.Model):
    __tablename__ = "regular_nav"
    id = db.Column(db.Integer, primary_key=True)
    regular_code = db.Column(db.Integer, db.ForeignKey("schemes_list.regular_code"))
    nav_data_date = db.Column(db.String(50))
    regular_nav_double_precision = db.Column(db.Numeric(10, 2))

db.create_all()


# with open('all_AMCs.csv', 'r') as csvfile:
#     csvreader = csv.reader(csvfile)
    
#     for row in csvreader:
#        # Access data from each row
#          # row[0] represents the value in the first column
#          # row[1] represents the value in the second column, and so on
#        print(row)  # Example: Print the value in the first column

import csv
from datetime import date

    # Get today's date
today = date.today()

    # Format the date as "DD-MM-YYYY"
formatted_date = today.strftime("%d-%m-%Y")

    # Print the formatted date
print(formatted_date)
formatted_date=str(formatted_date)

from datetime import timedelta
j_date = datetime.strptime(formatted_date, '%d-%m-%Y').date()
new_date = j_date - timedelta(days=1)
# Convert the incremented date back to string format
new_date_str = new_date.strftime('%d-%m-%Y')
print(new_date_str)

import csv
def init():
 with open('all_AMCs.csv', 'r') as csvfile:
    csvreader = csv.reader(csvfile)
    
    # Skip the heading row
    next(csvreader)
    
    # Skip the column header row
    next(csvreader)
    
    # Extract AMC names from each row
    amc_names = [row[1] for row in csvreader]
    
    # Print the AMC names
    print("AMC Names:")
    
    for amc_name in amc_names:
         cf= AMCs_list(amc_name=amc_name)
         db.session.add(cf)
         db.session.commit()

check_data= AMCs_list.query.all()
if len(check_data)==0:
    init()

def init2():

 with open("Scheme Code Direct_Regular.csv","r") as csvfile:
    csvreader = csv.reader(csvfile)
    next(csvreader)
    for row in csvreader:
        if 'aditya birla sun life' in row[0].lower():
            a1= schemes_list(amc_id=1,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'axis' in row[0].lower():
            a1= schemes_list(amc_id=2,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'bank of india' in row[0].lower():
            a1=schemes_list(amc_id=3,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'baroda bnp ' in row[0].lower():
            a1=schemes_list(amc_id=4,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'canara' in row[0].lower():
           a1= schemes_list(amc_id=5,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'dsp' in row[0].lower():
            a1=schemes_list(amc_id=6,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'edelweiss' in row[0].lower():
            a1=schemes_list(amc_id=7,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'franklin' in row[0].lower():
            a1=schemes_list(amc_id=8,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'hdfc' in row[0].lower():
            a1=schemes_list(amc_id=9,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'hsbc' in row[0].lower():
            a1=schemes_list(amc_id=10,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'icici prudential' in row[0].lower():
            a1=schemes_list(amc_id=11,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'idbi' in row[0].lower():
           a1= schemes_list(amc_id=12,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'idfc' in row[0].lower():
            a1=schemes_list(amc_id=13,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'iifl' in row[0].lower():
            a1=schemes_list(amc_id=14,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'indiabulls' in row[0].lower():
            a1=schemes_list(amc_id=15,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'invesco' in row[0].lower():
            a1=schemes_list(amc_id=16,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'iti' in row[0].lower():
           a1= schemes_list(amc_id=17,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'jm financial' in row[0].lower():
           a1= schemes_list(amc_id=18,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'kotak asset' in row[0].lower():
            a1=schemes_list(amc_id=19,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'lic' in row[0].lower():
            a1=schemes_list(amc_id=20,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'mahindra manulife' in row[0].lower():
            a1=schemes_list(amc_id=21,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'mirae asset' in row[0].lower():
            a1=schemes_list(amc_id=22,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'motilal oswal' in row[0].lower():
            a1=schemes_list(amc_id=23,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'navi' in row[0].lower():
            a1=schemes_list(amc_id=24,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'nippon' in row[0].lower():
            a1=schemes_list(amc_id=25,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'nj' in row[0].lower():
            a1=schemes_list(amc_id=26,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'pgim' in row[0].lower():
            a1=schemes_list(amc_id=27,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'ppfas' in row[0].lower():
            a1=schemes_list(amc_id=28,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'quant' in row[0].lower():
            a1=schemes_list(amc_id=29,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'quantum' in row[0].lower():
            a1=schemes_list(amc_id=30,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'samco' in row[0].lower():
            a1=schemes_list(amc_id=31,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'sbi' in row[0].lower():
            a1=schemes_list(amc_id=32,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'shriram' in row[0].lower():
            a1=schemes_list(amc_id=33,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'sundaram' in row[0].lower():
            a1=schemes_list(amc_id=34,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'tata' in row[0].lower():
            a1=schemes_list(amc_id=35,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'tauras' in row[0].lower():
            a1=schemes_list(amc_id=36,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'trust' in row[0].lower():
            a1=schemes_list(amc_id=37,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'union' in row[0].lower():
           a1= schemes_list(amc_id=38,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'uti' in row[0].lower():
            a1=schemes_list(amc_id=39,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        elif 'white oak capital' in row[0].lower():
            a1=schemes_list(amc_id=40,scheme_name=row[0],direct_code=row[1],regular_code=row[2])
        db.session.add(a1)
        db.session.commit()

check_data2= schemes_list.query.all()
if len(check_data2)==0:
    init2()


all_scheme_codes = mf.get_scheme_codes()
direct_codes=schemes_list.query.with_entities(schemes_list.direct_code)
# for i in direct_codes:
#     print(i[0])
print(type(direct_codes[0][0]))


def init3():
    
    direct_codes=schemes_list.query.with_entities(schemes_list.direct_code)
    for direct_code in  direct_codes:
       direct_code_value = direct_code[0]
       print(direct_code_value)
       nav_data=mf.get_scheme_historical_nav_for_dates(direct_code_value,'01-01-2013',formatted_date)
       list=nav_data['data']
       for j in list:
            if(len(j)==1):
                            pass
            else:
        
                cf=direct_nav(direct_code=direct_code_value,nav_data_date=j['date'],direct_nav_double_precision=j['nav'])
                db.session.add(cf)
                db.session.commit()
            

check_data3= direct_nav.query.all()
if len(check_data3)==0:
    init3()


def init4():
    
    regular_codes=schemes_list.query.with_entities(schemes_list.regular_code)
    for regular_code in  regular_codes:
       regular_code_value = regular_code[0]
       print(regular_code_value)
       nav_data=mf.get_scheme_historical_nav_for_dates(regular_code_value,'01-01-2013',formatted_date)
       list=nav_data['data']
       for j in list:
            
            if(len(j)==1):
                            pass
            else:
        
                cf=regular_nav(regular_code=regular_code_value,nav_data_date=j['date'],regular_nav_double_precision=j['nav'])
                db.session.add(cf)
                db.session.commit()

check_data4= regular_nav.query.all()
if len(check_data4)==0:
    init4()

def update_regular():
    from datetime import datetime
    dates = regular_nav.query.with_entities(regular_nav.nav_data_date).all()
    
    
    print(type(dates[0][0]))


     
    latest_date = None

    # Iterate over the dates and find the latest date
    for date in dates:
         date_str = str(date[0])
        #  print(type(date_str))

     # Convert the date string to a datetime object
         date = datetime.strptime(date_str, '%d-%m-%Y')

     # Check if it's the latest date
         if latest_date is None or date > latest_date:
             latest_date = date
             print(type(latest_date))
             print(formatted_date)
             

#     # Print the latest date
    if latest_date is not None:
          print("Latest date:", latest_date.strftime('%d-%m-%Y'))
          latest_date=(latest_date.strftime('%d-%m-%Y'))
#     # else:
#     #     print("No dates found.")
    regular_codes=schemes_list.query.with_entities(schemes_list.regular_code)
    for regular_code in  regular_codes:
     i = regular_code[0]
     
     
    
  
     nav_data=mf.get_scheme_historical_nav_for_dates(i,(new_date_str),(new_date_str))
     list=nav_data['data']
     for j in list:
                        if(len(j)==1):
                            pass
                        else:
                            cf2=regular_nav(regular_code=i,nav_data_date=j['date'],regular_nav_double_precision=j['nav'])
                            db.session.add(cf2)
                            db.session.commit()
                            print(cf2)
                                

                        

if(regular_nav.query.filter_by(nav_data_date=new_date_str).first()):
       print("hi2")
       
    
else:
   update_regular()



def update_direct():
    from datetime import datetime
    dates = direct_nav.query.with_entities(direct_nav.nav_data_date).all()
    
    
    print(type(dates[0][0]))


     
    latest_date = None

    # Iterate over the dates and find the latest date
    for date in dates:
         date_str = str(date[0])
        #  print(type(date_str))

     # Convert the date string to a datetime object
         date = datetime.strptime(date_str, '%d-%m-%Y')

     # Check if it's the latest date
         if latest_date is None or date > latest_date:
             latest_date = date
             print(type(latest_date))
             print(formatted_date)
             

#     # Print the latest date
    if latest_date is not None:
          print("Latest date:", latest_date.strftime('%d-%m-%Y'))
          latest_date=(latest_date.strftime('%d-%m-%Y'))
#     # else:
#     #     print("No dates found.")
    direct_codes=schemes_list.query.with_entities(schemes_list.direct_code)
    for direct_code in  direct_codes:
     i = direct_code[0]
     
     
    
  
     nav_data=mf.get_scheme_historical_nav_for_dates(i,(new_date_str),(new_date_str))
     list=nav_data['data']
     for j in list:
                        if(len(j)==1):
                            print("hello")
                        else:
                            cf2=direct_nav(direct_code=i,nav_data_date=j['date'],direct_nav_double_precision=j['nav'])
                            db.session.add(cf2)
                            db.session.commit()
                            print(cf2)
                                

                        

if(direct_nav.query.filter_by(nav_data_date=new_date_str).first()):
       print("hi")
       
    
else:
   update_direct()
            




import csv
nav_data=mf.get_scheme_historical_nav_for_dates(132989,(new_date_str),(new_date_str))
list=nav_data['data']
for j in list:
                      
# Data to be written to the CSV file
  if(len(j)==1):
                   data=["hello"]         
  else:
                   data=["bye"]


# File path of the CSV file
file_path = 'data.csv'

# Open the CSV file in write mode
with open(file_path, mode='a', newline='') as file:
    # Create a CSV writer object
    writer = csv.writer(file)

    # Write the data to the CSV file
    writer.writerows(data)

print('Data has been written to', file_path)

# -----------------------------------------------------------------------------------------------------------

# @app.route('/')
# def index():
#     return render_template('template.html')






# @app.route('/get_schemes', methods=['POST', 'GET'])
# def scheme_list():
#     scheme_list = []
#     #amcName=request.args.get("amcName")
#     amcName = "Aditya Birla Sun Life Mutual Fund "
#     id = AMCs_list.query.filter_by(amc_name=amcName).first()
#     amc_id = id.amc_id
#     print(amc_id)
#     id2 = schemes_list.query.filter_by(amc_id=amc_id).all()
    
#     for scheme in id2:
#         scheme_list.append(
#              scheme.scheme_name,
#             # Add other attributes as needed
#         )
    
#     return jsonify({'schemes of'+ amcName: scheme_list})

@app.route('/get_amcs', methods=['GET'])
def amc_list():
    amc=[]
    amc_names=AMCs_list.query.with_entities(AMCs_list.amc_name)
    h=1
    for i in  amc_names:
      
      j=i[0]
      print(j)
      amc.append({"amc_id":h,"amc_name":j})
      h+=1
    return  jsonify({'amcs': amc})



@app.route('/get_schemes', methods=['POST', 'GET'])
def scheme_list():
    scheme_list = []
    amcId=request.args.get("amc_id")
    # ----------------------------------------------------------------
    # amcName = "Aditya Birla Sun Life Mutual Fund "
    # id = AMCs_list.query.filter_by(amc_name=amcName).first()
    # amc_id = id.amc_id
    # print(amc_id)
    # -----------------------------------------------------------------
    id2 = schemes_list.query.filter_by(amc_id=amcId).all()
    
    for scheme in id2:
        scheme_list.append(
             scheme.scheme_name
            
        )
    
    return jsonify({'schemes ': scheme_list})


@app.route('/get_nav', methods=['POST', 'GET'])
def calculate():
    
    schemeId=request.args.get("scheme_id")
    
    scheme = schemes_list.query.filter_by(scheme_id=schemeId).first()
    scheme= schemes_list.query.filter_by(scheme_id=7).first()
    direct_code= scheme.direct_code
    regular_code=scheme.regular_code
    print("direct_code"+str(direct_code))
    print("regular_code"+str(regular_code))
    
    #start_date= request.args.get("start_date")
    #end_date= request.args.get("end_date")
    start_date="14-05-2022"
    #end_date="16-05-2021"
    end_date="29-05-2022"
    
    date1 = datetime.strptime(start_date, '%d-%m-%Y')
    date2 = datetime.strptime(end_date, '%d-%m-%Y')
    

# # Calculate the difference between the dates
#     difference = date2 - date1

# # Access the difference in days
#     days_difference = difference.days

#     print("n="+str(days_difference))
#     n= days_difference
    from datetime import datetime

    def days_in_year(year):
        """
        Get the number of days in a year.
        """
        return 366 if is_leap_year(year) else 365

    def is_leap_year(year):
        """
        Check if a year is a leap year.
        """
        return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)

    def yearsfrac_actual_actual(start_date, end_date):
        """
        Calculate the fractional number of years between two dates using the actual/actual method.
        """
        

        # Get the number of days in the start and end years
        start_year_days = days_in_year(start_date.year) - start_date.timetuple().tm_yday
        end_year_days = end_date.timetuple().tm_yday

        # Calculate the number of days between the two dates
        total_days = (end_date.year - start_date.year - 1) * 365 + start_year_days + end_year_days

        # Add the number of leap days between the years
        for year in range(start_date.year + 1, end_date.year):
            if is_leap_year(year):
                total_days += 1

        # Calculate the fractional number of years
        years = total_days / 365

        return years


    result = yearsfrac_actual_actual(date1, date2)
    print(result)



    start_date_dc=  direct_nav.query.filter_by(direct_code = direct_code, nav_data_date=start_date).first()
    end_date_dc= direct_nav.query.filter_by(direct_code = direct_code, nav_data_date=end_date).first()
    start_date_rc=  regular_nav.query.filter_by(regular_code = regular_code, nav_data_date=start_date).first()
    end_date_rc=  regular_nav.query.filter_by(regular_code = regular_code, nav_data_date=end_date).first()
    
    if start_date_dc is not None:
     sdd=start_date_dc.direct_nav_double_precision
    if end_date_dc is not None:
     edd=end_date_dc.direct_nav_double_precision
    if start_date_rc is not None:
     sdr=start_date_rc.regular_nav_double_precision
    if end_date_rc is not None:
     edr=end_date_rc.regular_nav_double_precision

    while start_date_dc is None:
         date = datetime.strptime(start_date, '%d-%m-%Y')

# Add 1 day
         if date<date2:
          
          new_start_date = date + timedelta(days=1)
         else:
          print("No data found")
          break

# Convert the new date to a string in the desired format
         start_date = new_start_date.strftime('%d-%m-%Y')
         start_date_dc=  direct_nav.query.filter_by(direct_code = direct_code, nav_data_date=start_date).first()
         if start_date_dc is not None:
          sdd=start_date_dc.direct_nav_double_precision
         else:
              start_date_dc = None



    while end_date_dc is None:
         date = datetime.strptime(end_date, '%d-%m-%Y')

# Add 1 day
         if date>date1:
          new_end_date = date - timedelta(days=1)
         else:
          print("NO data found")

# Convert the new date to a string in the desired format
         end_date = new_end_date.strftime('%d-%m-%Y')
         end_date_dc=  direct_nav.query.filter_by(direct_code = direct_code, nav_data_date=end_date).first()
         if end_date_dc is not None:
          edd=end_date_dc.direct_nav_double_precision
         else:
              end_date_dc=None


    
    while start_date_rc is None:
         date = datetime.strptime(start_date, '%d-%m-%Y')

# Add 1 day
         if date<date2:
          print("sd"+str(date))
          new_start_date = date + timedelta(days=1)
         else :
             print("No data found")
             break

# Convert the new date to a string in the desired format
         start_date = new_start_date.strftime('%d-%m-%Y')
         start_date_rc=  regular_nav.query.filter_by(regular_code = regular_code, nav_data_date=start_date).first()
         if start_date_rc is not None:
          sdr=start_date_rc.regular_nav_double_precision
         else:
              start_date_rc=None



    while end_date_rc is None:
         date = datetime.strptime(end_date, '%d-%m-%Y')

# Add 1 day
         if date> date1:
          new_end_date = date - timedelta(days=1)
         else:
             print("No data found")
             break

# Convert the new date to a string in the desired format
         end_date = new_end_date.strftime('%d-%m-%Y')
         end_date_rc=  regular_nav.query.filter_by(regular_code = regular_code, nav_data_date=end_date).first()
         if end_date_rc is not None:
          edr=end_date_rc.regular_nav_double_precision
         else:
              end_date_rc =None
         

   
        
    print("sd"+str(start_date_dc.direct_nav_double_precision))       
    print("ed"+str(end_date_dc.direct_nav_double_precision))
    print("sr"+str(start_date_rc.regular_nav_double_precision))
    print("er"+str(end_date_rc.regular_nav_double_precision))

    cagr_direct = ((float(edd/sdd))**(1/n))-1
    cagr_regular=((float(edr/sdr))**(1/n))-1

    cagr=[cagr_direct,cagr_regular]
    return jsonify({'cagr ': cagr})























if __name__ == "__main__":
    app.run(debug=True)
app.app_context()






