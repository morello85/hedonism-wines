import altair as alt # type: ignore
import streamlit as st
import matplotlib.pyplot as plt
import pandas as pd
import queries
import re
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

    primary_axis = alt.Axis(title='Values', grid=False)
	
    # Define custom color scale
    custom_color_scale = alt.Scale(domain=['stock_count', 'median_price'], range=['blue', 'red'])

    my_chart = alt.Chart(df).mark_trail().transform_fold(
    fold=['stock_count', 'median_price'], 
    as_=['legend', 'value']
).encode(
    x='import_date',
    y=alt.Y('max(value):Q',axis=primary_axis),
    color=alt.Color('legend:N', scale = custom_color_scale)
)
    st.altair_chart(my_chart, use_container_width=True
					)

# def visualise_stocks_and_median_values_by_code():
#     df = queries.stocks_and_median_values_by_code()

#     # Add a title to the Streamlit app
#     st.title('Median Price Check By Code')

#     primary_axis = alt.Axis(title='Values', grid=False)

#     # Define custom color scale
#     custom_color_scale = alt.Scale(domain=['stock_count', 'median_price'], range=['blue', 'red'])

#     # Create a text input box for filtering the codes (allowing multiple codes)
#     code_filter = st.text_area('Enter codes (comma-separated):', value='HED36140, HED85155')

#     # Strip any leading/trailing whitespace from the input and split by comma
#     code_filter = code_filter.strip()
#     codes_list = [code.strip() for code in code_filter.split(',')]

#     # Escape any special characters in the item codes for regex use
#     escaped_codes = [re.escape(code) for code in codes_list]

#     # Join the escaped codes into a single regex pattern that matches any of the codes
#     regex_pattern = '|'.join(escaped_codes)

#     # Ensure that the 'code' column is string type
#     df['code'] = df['code'].astype(str)

#     # Filter the DataFrame based on the selected codes
#     filtered_df = df[df['code'].str.contains(regex_pattern, case=False, na=False)]

#     # Ensure that the filtered DataFrame is not empty
#     if not filtered_df.empty:
#         line_chart = alt.Chart(filtered_df).mark_line().encode(
#             x='import_date',
#             y='median_price:Q',  # Plotting median_price for each code
#             color='code:N',  # Color by each code
#             tooltip=['code', 'import_date', 'median_price']  # Tooltip to show relevant info
#         )

#         # Display the chart using Streamlit
#         st.altair_chart(line_chart, use_container_width=True)
#     else:
#         st.write(f"No data found for the entered codes.")


def visualise_stocks_and_median_values_by_code():
    df = queries.stocks_and_median_values_by_code()

    # Add a title to the Streamlit app
    st.title('Median Price Check By Code')

    primary_axis = alt.Axis(title='Values', grid=False)

    # Define custom color scale
    custom_color_scale = alt.Scale(domain=['stock_count', 'median_price'], range=['blue', 'red'])

    # Create a text input box for filtering the codes (allowing multiple codes)
    code_filter = st.text_area('Enter codes (comma-separated):', value='HED36140, HED85155')

    # Strip any leading/trailing whitespace from the input and split by comma
    code_filter = code_filter.strip()
    codes_list = [code.strip() for code in code_filter.split(',')]

    # Escape any special characters in the item codes for regex use
    escaped_codes = [re.escape(code) for code in codes_list]

    # Join the escaped codes into a single regex pattern that matches any of the codes
    regex_pattern = '|'.join(escaped_codes)

    # Ensure that the 'code' column is string type
    df['code'] = df['code'].astype(str)

    # Filter the DataFrame based on the selected codes
    filtered_df = df[df['code'].str.contains(regex_pattern, case=False, na=False)]

    # Ensure that the filtered DataFrame is not empty
    if not filtered_df.empty:
        line_chart = alt.Chart(filtered_df).mark_line().encode(
            x='import_date',
            y='median_price:Q',  # Plotting median_price for each code
            color='code:N',  # Color by each code
            tooltip=['code', 'import_date', 'median_price']  # Tooltip to show relevant info
        )

        # Display the chart using Streamlit
        st.altair_chart(line_chart, use_container_width=True)
    else:
        st.write(f"No data found for the entered codes.")

def visualise_units_sold():
	df = queries.units_sold()

	# Add a title to the Streamlit app
	st.title('Previous Day Units Sold')
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
	    x=alt.X('title', sort='-y'),
	    y='price_gbp',
	    tooltip=['title', 'price_gbp']
	).interactive()

	# Display the chart using Streamlit Vega-Lite
	st.altair_chart(chart
	                #, use_container_width=True
	               )

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
visualise_stocks_and_median_values_by_code()
visualise_units_sold()
visualise_price_search()
