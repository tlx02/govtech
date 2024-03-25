import requests
import json
import pandas as pd
import datetime

# obtain restaurant_data.json
response = requests.get('https://raw.githubusercontent.com/Papagoat/brain-assessment/main/restaurant_data.json')
json_data = response.json()
restaurants = json_data[0]['restaurants']

# create restaurants dataframe
restaurants_data = {
    'Restaurant Id': [],
    'Restaurant Name': [],
    'Country': [],
    'City': [],
    'User Rating Votes': [],
    'User Aggregate Rating': [],
    'Cuisines': [],
}
restaurants_df = pd.DataFrame(restaurants_data)

# create events dataframe
events_data = {
    'Event Id': [],
    'Restaurant Id': [],
    'Restaurant Name': [],
    'Photo URL': [],
    'Event Title': [],
    'Event Start Date': [],
    'Event End Date': []
}
events_df = pd.DataFrame(events_data)

for r in restaurants:
    restaurant_id = r['restaurant']['id']
    restaurant_name = r['restaurant']['name']
    city = r['restaurant']['location']['city']
    country = r['restaurant']['location']['country_id']
    votes = r['restaurant']['user_rating']['votes']
    aggregate_rating = r['restaurant']['user_rating']['aggregate_rating']
    cuisines = r['restaurant']['cuisines']
    
    if 'zomato_events' in r['restaurant']:
        events = r['restaurant']['zomato_events']
        for event in events:
            event_id = event['event']['event_id']
            event_title = event['event']['title']
            if 'photos' in event['event']:
                photos = event['event']['photos']
                photo_url = event['event']['photos'][0]['photo']['url'] if len(photos) != 0 else None
            start_date = event['event']['start_date']
            end_date = event['event']['end_date']
        
            april_start_date = datetime.date(2019, 4, 1)
            april_end_date = datetime.date(2019, 4, 30)
            start_date_obj = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
            end_date_obj = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()

            # check if the event is in April 2019
            if ((april_start_date <= start_date_obj <= april_end_date)
                or (april_start_date <= end_date_obj <= april_end_date)
                or (start_date_obj <= april_start_date <= end_date_obj)):
            
                # insert new row into the events dataframe
                new_event_entry = {'Event Id': event_id, 'Restaurant Id': restaurant_id, 'Restaurant Name': restaurant_name, 
                                'Photo URL': photo_url, 'Event Title': event_title, 'Event Start Date': start_date,
                                'Event End Date': end_date
                                }
                new_event_df = pd.DataFrame([new_event_entry])
                events_df = pd.concat([events_df, new_event_df], axis = 0, ignore_index = True)
  
    # insert new row into the restaurants dataframe
    new_restaurant_entry = {'Restaurant Id': restaurant_id, 'Restaurant Name': restaurant_name, 'Country': country, 'City': city, 
                 'User Rating Votes': votes, 'User Aggregate Rating': aggregate_rating, 'Cuisines': cuisines}
    new_restaurant_df = pd.DataFrame([new_restaurant_entry])
    restaurants_df = pd.concat([restaurants_df, new_restaurant_df], axis = 0, ignore_index = True)

restaurants_df.to_csv('restaurants.csv')
events_df.to_csv('restaurant_events.csv')