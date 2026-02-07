from pathlib import Path
import altair as alt  # type: ignore
import streamlit as st

import queries_athena as queries


@st.cache_data(ttl=300, show_spinner=False)
def load_discounted_items():
    return queries.query_discounted_items()


@st.cache_data(ttl=300, show_spinner=False)
def load_stocks_and_median_values():
    return queries.stocks_and_median_values()


@st.cache_data(ttl=300, show_spinner=False)
def load_stocks_and_median_values_by_code(codes: tuple[str, ...]):
    return queries.stocks_and_median_values_by_code(list(codes))


@st.cache_data(ttl=300, show_spinner=False)
def load_previous_day_units_sold():
    return queries.previous_day_units_sold()


@st.cache_data(ttl=300, show_spinner=False)
def load_units_sold_by_date(target_date_iso: str):
    return queries.units_sold_by_date(target_date_iso)


@st.cache_data(ttl=300, show_spinner=False)
def load_price_search():
    return queries.price_search()


def last_refresh_message() -> str:
    """Get the last refresh message for the dashboard."""
    timestamp_path = Path(__file__).resolve().parent / "last_refresh.txt"
    if timestamp_path.exists():
        timestamp = timestamp_path.read_text(encoding="utf-8").strip()
        if timestamp:
            return f"this data was last refreshed on {timestamp}"
    return "this data was last refreshed on unavailable"


def visualise_discounted_items():
    """Visualize discounted items."""
    df = load_discounted_items()

    st.caption(last_refresh_message())
    st.title('Discounts')

    if df.empty:
        st.write("Sorry, no available discounts today")
    else:
        left_value = st.number_input('Enter lower discount bound:', min_value=0, value=100, step=50)
        right_value = st.number_input('Enter upper discount bound:', min_value=0, value=5000, step=50)
        discount_perc_value = st.number_input('Enter discount percentage (%):', min_value=0.0, value=10.0, step=1.0)
        title_filter = st.text_input('Enter title:', value='Karuizawa')

        discount_range = st.slider(
            'Select discount range (GBP)',
            min_value=100,
            max_value=2000,
            value=(int(left_value), int(right_value)),
            step=100,
        )

        filtered_df = df[
            (df['discount'] >= discount_range[0])
            & (df['discount'] <= discount_range[1])
            & (df['perc_saving'] > discount_perc_value)
            & (df['title'].str.contains(title_filter, case=False, na=False))
        ]

        filtered_df = filtered_df.sort_values(by='discount', ascending=False)
        st.data_editor(
            filtered_df,
            column_config={
                "price_changes_count": st.column_config.NumberColumn(
                    "price_changes_count",
                    help="Number of distinct historical median prices observed for this code",
                ),
                "url": st.column_config.LinkColumn(
                    "link",
                    help="Click to access the whisky page on hedonism wines",
                    validate="^https?://.*$",
                    max_chars=100,
                    display_text="https://(.*?)\\.streamlit\\.app",
                )
            },
            hide_index=True,
        )

        chart = alt.Chart(filtered_df).mark_bar().encode(
            x=alt.X('title', sort='-y'),
            y='current_price',
            tooltip=['title', 'current_price'],
        ).interactive()

        st.altair_chart(chart)


def visualise_stocks_and_median_values():
    """Visualize stock count and median price."""
    df = load_stocks_and_median_values()

    st.title('Stock and Median Price Check')

    if df.empty:
        st.info("No stock and median price data available.")
        return

    primary_axis = alt.Axis(title='Values', grid=False)
    custom_color_scale = alt.Scale(domain=['stock_count', 'median_price'], range=['blue', 'red'])

    my_chart = alt.Chart(df).mark_trail().transform_fold(
        fold=['stock_count', 'median_price'],
        as_=['legend', 'value'],
    ).encode(
        x='import_date',
        y=alt.Y('max(value):Q', axis=primary_axis),
        color=alt.Color('legend:N', scale=custom_color_scale),
    )
    st.altair_chart(my_chart, use_container_width=True)


def visualise_stocks_and_median_values_by_code():
    """Visualize median price by code."""
    st.title('Median Price Check By Code')

    code_filter = st.text_area('Enter codes (comma-separated):', value='HED36140, HED85155')
    code_filter = code_filter.strip()

    if not code_filter:
        st.info("Enter one or more codes to view code-level median prices.")
        return

    codes_list = [code.strip() for code in code_filter.split(',') if code.strip()]
    codes_tuple = tuple(dict.fromkeys(codes_list))
    df = load_stocks_and_median_values_by_code(codes_tuple)

    if df.empty or 'code' not in df.columns:
        st.info("No code-level median price data available.")
        return

    filtered_df = df.copy()

    if not filtered_df.empty:
        line_chart = alt.Chart(filtered_df).mark_line().encode(
            x='import_date',
            y='median_price:Q',
            color='code:N',
            tooltip=['code', 'import_date', 'median_price'],
        )

        st.altair_chart(line_chart, use_container_width=True)
    else:
        st.write("No data found for the entered codes.")


def visualise_previous_day_units_sold():
    """Visualize previous day units sold."""
    df = load_previous_day_units_sold()

    st.title('Previous Day Units Sold')

    if df.empty:
        st.info("No units sold data available for the previous day.")
        return
    st.data_editor(
        df,
        column_config={
            "price_changes_count": st.column_config.NumberColumn(
                "price_changes_count",
                help="Number of distinct historical median prices observed for this code",
            ),
            "url": st.column_config.LinkColumn(
                "link",
                help="Click to access the whisky page on hedonism wines",
                validate="^https?://.*$",
                max_chars=100,
                display_text="https://(.*?)\\.streamlit\\.app",
            )
        },
        hide_index=True,
    )

    chart = alt.Chart(df).mark_bar().encode(
        x=alt.X('title', sort='-y'),
        y='price_gbp',
        tooltip=['title', 'price_gbp'],
    ).interactive()

    st.altair_chart(chart)


def visualise_units_sold_by_date():
    """Visualize units sold for a selected date."""
    st.title('Units Sold by Selected Date')
    target_date = st.date_input('Select date to analyse units sold')

    df = load_units_sold_by_date(target_date.isoformat())
    if df.empty:
        st.info(f'No units sold data available for {target_date.isoformat()}.')
        return

    st.data_editor(
        df,
        column_config={
            "price_changes_count": st.column_config.NumberColumn(
                "price_changes_count",
                help="Number of distinct historical median prices observed for this code",
            ),
            "url": st.column_config.LinkColumn(
                "link",
                help="Click to access the whisky page on hedonism wines",
                validate="^https?://.*$",
                max_chars=100,
                display_text="https://(.*?)\\.streamlit\\.app",
            )
        },
        hide_index=True,
    )

    chart = alt.Chart(df).mark_bar().encode(
        x=alt.X('title', sort='-y'),
        y='price_gbp',
        tooltip=['title', 'price_gbp'],
    ).interactive()

    st.altair_chart(chart)


def visualise_price_search():
    """Visualize price search."""
    df = load_price_search()

    st.title('Price Search')

    if df.empty:
        st.info("No current stock pricing data available.")
        return

    left_value = st.number_input('Enter left value:', min_value=0, value=1000, step=500, key=1)
    right_value = st.number_input('Enter right value:', min_value=0, value=20000, step=500, key=2)
    title_filter = st.text_input('Enter title:', value='Karuizawa', key=3)

    price_range = st.slider(
        'Select price range (GBP)',
        min_value=0,
        max_value=700000,
        value=(int(left_value), int(right_value)),
        step=1000,
    )

    filtered_df = df[
        (df['price_gbp'] >= price_range[0])
        & (df['price_gbp'] <= price_range[1])
        & (df['title'].str.contains(title_filter, case=False, na=False))
    ]

    filtered_df = filtered_df.sort_values(by='price_gbp', ascending=False)

    st.data_editor(
        filtered_df,
        column_config={
            "price_changes_count": st.column_config.NumberColumn(
                "price_changes_count",
                help="Number of distinct historical median prices observed for this code",
            ),
            "url": st.column_config.LinkColumn(
                "link",
                help="Click to access the whisky page on hedonism wines",
                validate="^https?://.*$",
                max_chars=100,
                display_text="https://(.*?)\\.streamlit\\.app",
            )
        },
        hide_index=True,
    )

    chart = alt.Chart(filtered_df).mark_bar().encode(
        x=alt.X('title', sort='-y'),
        y='price_gbp',
        tooltip=['title', 'price_gbp'],
    ).interactive()

    st.altair_chart(chart)


def main():
    """Run one selected visualization to avoid querying all datasets per rerun."""
    selected_view = st.sidebar.radio(
        "Dashboard view",
        (
            'Discounts',
            'Stock and Median Price Check',
            'Median Price Check By Code',
            'Previous Day Units Sold',
            'Units Sold by Selected Date',
            'Price Search',
        ),
    )

    view_map = {
        'Discounts': visualise_discounted_items,
        'Stock and Median Price Check': visualise_stocks_and_median_values,
        'Median Price Check By Code': visualise_stocks_and_median_values_by_code,
        'Previous Day Units Sold': visualise_previous_day_units_sold,
        'Units Sold by Selected Date': visualise_units_sold_by_date,
        'Price Search': visualise_price_search,
    }
    view_map[selected_view]()


if __name__ == "__main__":
    main()
