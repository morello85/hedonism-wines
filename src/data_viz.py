import altair as alt
import streamlit as st
import matplotlib.pyplot as plt
import pandas as pd
import queries
import duckdb
import os

# Read the database file path from the environment variable
#db_path = os.getenv('DB_PATH')


# Specify the file path for the DuckDB database
db_path = '/Users/MacUser/hedonism-wines_app/database.db'  # Example path, replace with your desired path

# Establish a connection to an in-memory DuckDB database
conn = duckdb.connect(database=db_path, read_only=False)


def visualise_discounted_items():
    # Add a title to the Streamlit app
    st.title('Discounts')

    df = queries.query_discounted_items()

    if df.empty:
        st.write("Sorry, no available discounts today")

    else:
        # Display the DataFrame with clickable titles
        # for index, row in df.iterrows():
        #     st.write(f'<a href="{row["url"]}" target="_blank">{row["title"]}</a>')
        #     st.write(f'Price: {row["current_minimum_price"]}')
        st.data_editor(
            df,
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
        chart = alt.Chart(df).mark_bar().encode(
            x='title',
            y='current_minimum_price',
            tooltip=['title', 'current_minimum_price']
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
	primary_y_axis = alt.Axis(title='Stock Count', grid=False)
	secondary_y_axis = alt.Axis(title='Median Price', grid=False, orient='right')

	# Line chart for stock count
	line_chart_stock_count = alt.Chart(df).mark_point(color='blue').encode(
	    x='import_date',
	    y=alt.Y('stock_count:Q', axis=primary_y_axis),
	)

	# Line chart for median price
	line_chart_median_price = alt.Chart(df).mark_point(color='red').encode(
	    x='import_date',
	    y=alt.Y('median_price:Q', axis=secondary_y_axis),
	)

	# Combine both charts
	combined_chart = line_chart_stock_count + line_chart_median_price

	# Display the chart using Streamlit Vega-Lite
	st.altair_chart(combined_chart, use_container_width=True)


def visualise_price_search():

	df = queries.price_search()

	# Add a title to the Streamlit app
	st.title('Price Search')

	# Create text input boxes for the left and right values of the slider
	left_value = st.text_input('Enter left value:', value='0')
	right_value = st.text_input('Enter right value:', value='5000')

	# Create a text input box for filtering the title
	title_filter = st.text_input('Enter title:', value='Yamazaki')

	# Convert the input values to integers
	left_value = int(left_value)
	right_value = int(right_value)

	# Create a slider for selecting the price range
	price_range = st.slider('Select price range (GBP)', min_value=0, max_value=700000, value=(left_value, right_value), step=1000)

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