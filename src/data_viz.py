import altair as alt
import streamlit as st
import matplotlib.pyplot as plt
import pandas as pd
import queries
import duckdb
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Read the database file path from the environment variable
db_path = os.getenv('DB_PATH')

# Establish a connection to an in-memory DuckDB database
conn = duckdb.connect(database=db_path, read_only=False)

def visualise_discounted_items():
    # Add a title to the Streamlit app
	df = queries.query_discounted_items()

	st.title('Discounts')

	if df.empty:
		st.write("Sorry, no available discounts today")
	
	else:
        # Display the DataFrame with clickable titles
        # for index, row in df.iterrows():
        #     st.write(f'<a href="{row["url"]}" target="_blank">{row["title"]}</a>')
        #     st.write(f'Price: {row["current_minimum_price"]}')

		# Create text input boxes for the left and right values of the slider
		left_value = st.text_input('Enter lower discount bound:', value='100')
		right_value = st.text_input('Enter upper discount bound:', value='5000')
		discount_perc_value = st.text_input('Enter discount percentage (%):',value='10')

		# Create a text input box for filtering the title
		title_filter = st.text_input('Enter title:', value='Karuizawa')

		# Convert the input values to integers
		left_value = int(left_value)
		right_value = int(right_value)

		# Convert discount percentage value to float
		discount_perc_value = float(discount_perc_value)
		
		# Create a slider for selecting the price range
		discount_range = st.slider('Select discount range (GBP)', min_value=100, max_value=2000, 
							value=(left_value, right_value), step=100)

		# Filter the DataFrame based on the selected price range and title filter
		filtered_df = df[
			(df['discount'] >= discount_range[0]) & 
			(df['discount'] <= discount_range[1]) &
            (df['perc_saving'] > discount_perc_value) &  # Adjusted filter condition
			(df['title'].str.contains(title_filter, case=False))
		]
		
		filtered_df = filtered_df.sort_values(by='discount', ascending=False)
		st.data_editor(
            filtered_df,
            column_config={
                "url": st.column_config.LinkColumn(
                    "link",
                    help="Click to access the whisky page on hedonism wines",
                    validate="^https://[a-z]+\.streamlit\.app$",
                    max_chars=100,
                    display_text="https://(.*?)\.streamlit\.app"
                )
            },
            hide_index=True,
        )
        # Create Altair chart with tooltips
		chart = alt.Chart(filtered_df).mark_bar().encode(
			x=alt.X('title', sort='-y'),
            y='current_price',
            tooltip=['title', 'current_price']
        ).interactive()

        # Display the chart using Streamlit Vega-Lite
		st.altair_chart(chart
                        # , use_container_width=True
                        )

def visualise_stocks_and_median_values():
    df = queries.stocks_and_median_values()

    # Add a title to the Streamlit app
    st.title('Stock and Median Price Check')

    # Create Altair chart
    primary_y_axis = alt.Axis(title='Stock Count / Total Availability', grid=False)

    # Line chart for stock count
    line_chart_stock_count = alt.Chart(df).mark_point().encode(
        x='import_date',
        y=alt.Y('stock_count:Q', axis=primary_y_axis),
        color=alt.Color('stock_count:Q', scale=alt.Scale(scheme='darkblue'), legend=alt.Legend(title='Stock Count'))
    )

    # Line chart for median price 
    line_chart_median_price = alt.Chart(df).mark_point().encode(
        x='import_date',
        y=alt.Y('median_price:Q', axis=alt.Axis(title='Median Price', grid=False, orient='right')),
        color=alt.Color('median_price:Q', scale=alt.Scale(scheme='goldred'), legend=alt.Legend(title='Median Price'))
    )

    # Line chart for availability
    line_chart_availability = alt.Chart(df).mark_point().encode(
        x='import_date',
        y=alt.Y('total_availability:Q'),
        color=alt.Color('total_availability:Q', scale=alt.Scale(scheme='bluegreen'), legend=alt.Legend(title='Total Availability'))
    )

    combined_chart = (line_chart_stock_count + line_chart_median_price + line_chart_availability)

    # Display the chart using Streamlit Vega-Lite
    st.altair_chart(combined_chart, use_container_width=True)


# def visualise_stocks_and_median_values():

# 	df = queries.stocks_and_median_values()

# 	# Add a title to the Streamlit app
# 	st.title('Stock and Median Price Check')

# 	# Create Altair chart
# 	primary_y_axis = alt.Axis(title='Stock Count / Total Availability', grid=False)
# 	#secondary_y_axis = alt.Axis(title='Median Price', grid=False, orient='right')

# 	# Line chart for stock count
# 	line_chart_stock_count = alt.Chart(df).mark_point(color='blue').encode(
# 	    x='import_date',
# 	    y=alt.Y('stock_count:Q', axis=primary_y_axis),
# 	)
# 	# Line chart for median price 
# 	line_chart_median_price = alt.Chart(df).mark_point(color='red').encode(
#      x='import_date',
#         y=alt.Y('median_price:Q', axis=alt.Axis(title='Median Price', grid=False, orient='right')),
#     )
# 	# Line chart for availability
# 	line_chart_availability = alt.Chart(df).mark_point(color='green').encode(
#         x='import_date',
#         y=alt.Y('total_availability:Q'
# 				),
#     )
# 	combined_chart = (line_chart_stock_count + line_chart_median_price + line_chart_availability)
# 	# Display the chart using Streamlit Vega-Lite
# 	st.altair_chart(combined_chart, use_container_width=True)

def visualise_price_search():

	df = queries.price_search()

	# Add a title to the Streamlit app
	st.title('Price Search')

	# Create text input boxes for the left and right values of the slider
	left_value = st.text_input('Enter left value:', value='1000',key=1)
	right_value = st.text_input('Enter right value:', value='20000', key=2)

	# Create a text input box for filtering the title
	title_filter = st.text_input('Enter title:', value='Karuizawa',key=3)

	# Convert the input values to integers
	left_value = int(left_value)
	right_value = int(right_value)

	# Create a slider for selecting the price range
	price_range = st.slider('Select price range (GBP)', min_value=0, max_value=700000, 
						 value=(left_value, right_value), step=1000)

	# Filter the DataFrame based on the selected price range and title filter
	filtered_df = df[
	    (df['price_gbp'] >= price_range[0]) & 
	    (df['price_gbp'] <= price_range[1]) &
	    (df['title'].str.contains(title_filter, case=False))
	]
    
	filtered_df = filtered_df.sort_values(by='price_gbp', ascending=False)

	st.data_editor(
        filtered_df,
        column_config={
            "url": st.column_config.LinkColumn(
                "link",
                help="Click to access the whisky page on hedonism wines",
                validate="^https://[a-z]+\.streamlit\.app$",
                max_chars=100,
                display_text="https://(.*?)\.streamlit\.app"
            )
        },
        hide_index=True,
        )

	# Create Altair chart with tooltips
	chart = alt.Chart(filtered_df).mark_bar().encode(
	    x=alt.X('title', sort='-y'),
	    y='price_gbp',
	    tooltip=['title', 'price_gbp']
	).interactive()

	# Display the chart using Streamlit Vega-Lite
	st.altair_chart(chart
	                #, use_container_width=True
	               )

conn.close()


visualise_discounted_items()
visualise_stocks_and_median_values()
visualise_price_search()