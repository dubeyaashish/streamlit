import streamlit as st
import pandas as pd
import pymongo
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
from bson import ObjectId
import numpy as np
from streamlit_folium import folium_static
import folium
from folium.plugins import MarkerCluster
from streamlit_autorefresh import st_autorefresh
import geopandas as gpd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
import os

# ---------------------- Helper Functions ---------------------- #
def safe_str(val):
    """Safely convert a value to a string."""
    if pd.isnull(val):
        return "N/A"
    try:
        return str(val)
    except Exception:
        return "N/A"

class MongoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, ObjectId):
            return str(obj)
        return json.JSONEncoder.default(self, obj)

def format_date(dt):
    return dt.strftime('%Y-%m-%d %H:%M') if pd.notnull(dt) else "N/A"

# ---------------------- Page Configuration ---------------------- #
st.set_page_config(
    page_title="Job Management Dashboard",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded"
)
st_autorefresh(interval=600 * 1000, limit=None, key="datarefresh")

# ---------------------- Custom CSS ---------------------- #
st.markdown("""
<style>
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    .stTabs [data-baseweb="tab-list"] { gap: 10px; }
    .stTabs [data-baseweb="tab"] {
        padding: 10px 20px;
        border-radius: 5px 5px 0px 0px;
        font-weight: 500;
    }
    .stTabs [aria-selected="true"] {
        background-color: #0078ff;
        color: white;
    }
    .metric-card {
        background-color: #f8f9fa;
        border-radius: 10px;
        padding: 15px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.1);
    }
    .metric-value {
        font-size: 32px;
        font-weight: bold;
        color: #0078ff;
    }
    .metric-title {
        font-size: 14px;
        color: #6c757d;
    }
    .dashboard-title {
        font-weight: 700;
        padding-bottom: 10px;
        border-bottom: 2px solid #f0f2f6;
        margin-bottom: 20px;
    }
    [data-testid="stMetricDelta"] > div:nth-child(1) { justify-content: center; }
    [data-testid="stMetricValue"] > div { justify-content: center; }
    .stMarkdown p { margin-bottom: 0.5rem; }
    .priority-high { color: #ff4b4b; font-weight: bold; }
    .priority-medium { color: #ffa500; font-weight: bold; }
    .priority-low { color: #32cd32; font-weight: bold; }
</style>
""", unsafe_allow_html=True)
load_dotenv()

# ---------------------- Database Connection ---------------------- #
def get_database_connection():
    connection_string = os.getenv("MONGODB_CONNECTION_STRING")
    if not connection_string:
        connection_string = st.secrets.get("MONGODB_CONNECTION_STRING")
    if not connection_string:
        raise ValueError("MongoDB connection string not found in environment variables or secrets.")
    
    client = pymongo.MongoClient(connection_string)
    return client["prod"]

db = get_database_connection()

# ---------------------- Data Fetching Functions ---------------------- #
@st.cache_data(ttl=3600, show_spinner=False)
def get_job_data(start_date, end_date):
    start_date_obj = datetime.combine(start_date, datetime.min.time())
    end_date_obj = datetime.combine(end_date, datetime.max.time())
    
    pipeline = [
        {"$match": {"createdAt": {"$gte": start_date_obj, "$lte": end_date_obj}}},
        {"$lookup": {
            "from": "JobLocation",
            "localField": "jobLocationID",
            "foreignField": "_id",
            "as": "jobLocation"
        }},
        {"$unwind": {"path": "$jobLocation", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "User",
            "localField": "createdByID",
            "foreignField": "_id",
            "as": "createdBy"
        }},
        {"$unwind": {"path": "$createdBy", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "SettingCompany",
            "localField": "settingCompanyID",
            "foreignField": "_id",
            "as": "settingCompany"
        }},
        {"$unwind": {"path": "$settingCompany", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "Customer",
            "localField": "jobLocation.customerID",
            "foreignField": "_id",
            "as": "customer"
        }},
        {"$unwind": {"path": "$customer", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "TechnicianProfile",
            "localField": "technicianProfileIDs",
            "foreignField": "_id",
            "as": "technicians"
        }},
        {"$project": {
            "_id": 1,
            "jobNo": "$no",
            "status": 1,
            "type": 1,
            "priority": 1,
            "appointmentTime": 1,
            "createdAt": 1,
            "updatedAt": 1,
            "customerContact": 1,
            "locationName": "$jobLocation.name",
            "locationStatus": "$jobLocation.status",
            "locationType": "$jobLocation.type",
            "locationAddress": "$jobLocation.address",
            "locationSubDistrict": "$jobLocation.subDistrict",
            "locationDistrict": "$jobLocation.district",
            "locationProvince": "$jobLocation.province",
            "locationPostalCode": "$jobLocation.postalCode",
            "locationContactName": {
                "$concat": [
                    {"$ifNull": ["$jobLocation.contactFirstName", ""]},
                    " ",
                    {"$ifNull": ["$jobLocation.contactLastName", ""]}
                ]
            },
            "locationContactPhone": "$jobLocation.contactPhone",
            "locationCoordinates": "$jobLocation.location.coordinates",
            "createdByName": {
                "$concat": [
                    {"$ifNull": ["$createdBy.firstName", ""]},
                    " ",
                    {"$ifNull": ["$createdBy.lastName", ""]}
                ]
            },
            "companyName": "$settingCompany.name",
            "customerName": "$customer.name",
            "customerPhone": "$customer.phone",
            "customerEmail": "$customer.email",
            "customerType": "$customer.type",
            "customerStatus": "$customer.status",
            "technicians": 1,
            "isManualFindTechnician": 1,
            "isSendRequest": 1,
            "isEditable": 1,
            "isQcJob": 1,
            "isReview": 1,
            "isSlaInRisk": 1,
            "isSlaInFail": 1,
            "pauseTime": 1,
            "numOfHourSla": 1
        }}
    ]
    
    try:
        results = list(db.Job.aggregate(pipeline))
        # Flatten technician profiles using a list comprehension
        flattened_data = []
        for job in results:
            technicians = job.pop('technicians', [])
            technician_names = [
                " ".join([safe_str(tech.get("firstName", "")), safe_str(tech.get("lastName", ""))]).strip()
                for tech in technicians if isinstance(tech, dict) and (tech.get("firstName") or tech.get("lastName"))
            ]
            job['technician_names'] = ', '.join(technician_names) if technician_names else "N/A"
            flattened_data.append(job)
        df = pd.DataFrame(flattened_data)
        del flattened_data, results
        
        if not df.empty:
            for field in ['createdAt', 'updatedAt', 'appointmentTime']:
                if field in df.columns:
                    df[field] = pd.to_datetime(df[field], errors='coerce')
            df['locationProvince'] = df['locationProvince'].fillna('Unknown')
            df['locationDistrict'] = df['locationDistrict'].fillna('Unknown')
            df['locationSubDistrict'] = df['locationSubDistrict'].fillna('Unknown')
            if 'locationCoordinates' in df.columns:
                df['lon'] = df['locationCoordinates'].apply(lambda x: x[0] if isinstance(x, list) and len(x) == 2 else None)
                df['lat'] = df['locationCoordinates'].apply(lambda x: x[1] if isinstance(x, list) and len(x) == 2 else None)
            for col in ['pauseTime', 'numOfHourSla']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
        return df
    except Exception as e:
        st.error(f"Error retrieving job data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def get_review_data(start_date, end_date):
    start_date_obj = datetime.combine(start_date, datetime.min.time())
    end_date_obj = datetime.combine(end_date, datetime.max.time())
    
    pipeline = [
        {"$match": {"createdAt": {"$gte": start_date_obj, "$lte": end_date_obj}}},
        {"$lookup": {
            "from": "TechnicianProfile",
            "localField": "technicianProfileIDs",
            "foreignField": "_id",
            "as": "technicianProfiles"
        }},
        {"$unwind": {"path": "$technicianProfiles", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "Job",
            "localField": "jobID",
            "foreignField": "_id",
            "as": "jobDetails"
        }},
        {"$unwind": {"path": "$jobDetails", "preserveNullAndEmptyArrays": True}},
        {"$addFields": {"jobNo": "$jobDetails.no"}}
    ]
    
    try:
        results = list(db.CustomerReview.aggregate(pipeline))
        df_reviews = pd.DataFrame(results)
        if not df_reviews.empty:
            for field in ['time', 'manner', 'knowledge', 'overall', 'recommend']:
                if field in df_reviews.columns:
                    df_reviews[field] = pd.to_numeric(df_reviews[field], errors='coerce')
            if 'createdAt' in df_reviews.columns:
                df_reviews['createdAt'] = pd.to_datetime(df_reviews['createdAt'], errors='coerce')
            df_reviews['technician_name'] = df_reviews['technicianProfiles'].apply(
                lambda tech: " ".join([safe_str(tech.get('firstName', '')), safe_str(tech.get('lastName', ''))]).strip()
                if isinstance(tech, dict) and (tech.get('firstName') or tech.get('lastName')) else "Unknown"
            )
        return df_reviews
    except Exception as e:
        st.error(f"Error retrieving review data: {e}")
        return pd.DataFrame()

# ---------------------- New Helper: Get Team Leaders ---------------------- #
@st.cache_data(ttl=3600, show_spinner=False)
def get_team_leaders():
    """
    Retrieve a list of team leader names from the TechnicianProfile collection
    where the position is "TEAM_LEADER".
    """
    try:
        team_leaders_cursor = db.TechnicianProfile.find({"position": "TEAM_LEADER"})
        team_leaders = []
        for leader in team_leaders_cursor:
            name = " ".join([safe_str(leader.get("firstName", "")), safe_str(leader.get("lastName", ""))]).strip()
            if name:
                team_leaders.append(name)
        # Return unique names sorted alphabetically
        return sorted(list(set(team_leaders)))
    except Exception as e:
        st.error(f"Error retrieving team leaders: {e}")
        return []

# ---------------------- SIDEBAR Setup ---------------------- #
st.sidebar.image("https://bi.ruu-d.com/logo.png", width=100)
st.sidebar.title("Job Management Dashboard")

today = datetime.now().date()
default_start_date = today - timedelta(days=30)
with st.sidebar.expander("Date Range", expanded=True):
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", default_start_date)
    with col2:
        end_date = st.date_input("End Date", today)
    
    if start_date > end_date:
        st.error("Error: End date must be after start date.")
        st.stop()

st.sidebar.markdown("### Quick Select")
col1, col2 = st.sidebar.columns(2)
with col1:
    if st.button("Last 7 Days"):
        start_date = today - timedelta(days=7)
        end_date = today
        st.experimental_rerun()
with col2:
    if st.button("Last 30 Days"):
        start_date = today - timedelta(days=30)
        end_date = today
        st.experimental_rerun()
col1, col2 = st.sidebar.columns(2)
with col1:
    if st.button("This Month"):
        start_date = today.replace(day=1)
        end_date = today
        st.experimental_rerun()
with col2:
    if st.button("Last Month"):
        last_month = today.replace(day=1) - timedelta(days=1)
        start_date = last_month.replace(day=1)
        end_date = last_month
        st.experimental_rerun()

st.sidebar.markdown(f"**Selected Period:**  \n{start_date.strftime('%d %b, %Y')} to {end_date.strftime('%d %b, %Y')}")

# ---------------------- Data Loading ---------------------- #
with st.spinner("Loading real‑time data..."):
    df_jobs = get_job_data(start_date, end_date)
    df_reviews = get_review_data(start_date, end_date)

if df_jobs.empty:
    st.warning("No job data available for the selected date range.")
    st.stop()

for col in ['jobNo', 'status', 'locationName', 'locationAddress', 'locationDistrict', 'locationProvince']:
    if col in df_jobs.columns:
        df_jobs[col] = df_jobs[col].apply(safe_str)

# ---------------------- Sidebar Filters ---------------------- #
with st.sidebar.expander("Filters", expanded=True):
    selected_status = st.selectbox("Job Status", ['All'] + sorted(df_jobs['status'].unique().tolist())) if 'status' in df_jobs.columns else 'All'
    selected_type = st.selectbox("Job Type", ['All'] + sorted(df_jobs['type'].unique().tolist())) if 'type' in df_jobs.columns else 'All'
    selected_priority = st.selectbox("Priority", ['All'] + sorted(df_jobs['priority'].unique().tolist())) if 'priority' in df_jobs.columns else 'All'
    selected_province = st.selectbox("Province", ['All'] + sorted(df_jobs['locationProvince'].unique().tolist())) if 'locationProvince' in df_jobs.columns else 'All'
    
    # New filter: Technician / Team Leader
    team_leaders = get_team_leaders()
    selected_team_leader = st.selectbox("Team Leader", ['All'] + team_leaders)

# Apply filters to jobs dataframe
filtered_df = df_jobs.copy()
if selected_status != 'All':
    filtered_df = filtered_df[filtered_df['status'] == selected_status]
if selected_type != 'All':
    filtered_df = filtered_df[filtered_df['type'] == selected_type]
if selected_priority != 'All':
    filtered_df = filtered_df[filtered_df['priority'] == selected_priority]
if selected_province != 'All':
    filtered_df = filtered_df[filtered_df['locationProvince'] == selected_province]
# Filter jobs by selected team leader
if selected_team_leader != 'All':
    # This checks if the team leader's name is a substring of the job's technician_names field
    filtered_df = filtered_df[filtered_df['technician_names'].str.contains(selected_team_leader, na=False)]

# ---------------------- Tab Layout ---------------------- #
tab_overview, tab_geo, tab_tech = st.tabs(["📊 Overview", "🗺️ Geographic Analysis", "👨‍🔧 Technician Performance"])

# ---------------------- Tab 1: Overview ---------------------- #
def show_overview():
    st.markdown("<h2 class='dashboard-title'>Job Overview Dashboard</h2>", unsafe_allow_html=True)
    total_jobs = len(filtered_df)
    period_length = (end_date - start_date).days + 1
    previous_start = start_date - timedelta(days=period_length)
    previous_end = start_date - timedelta(days=1)
    previous_df = get_job_data(previous_start, previous_end)
    previous_total = len(previous_df)
    change_pct = ((total_jobs - previous_total) / previous_total * 100) if previous_total > 0 else 0

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label="Total Jobs", value=f"{total_jobs:,}", delta=f"{change_pct:.1f}% vs previous period")
    with col2:
        open_statuses = ['WAITINGJOB', 'WORKING', 'PENDING']
        open_jobs = filtered_df[filtered_df['status'].isin(open_statuses)].shape[0]
        st.metric(label="Open Jobs", value=f"{open_jobs:,}", delta=f"{open_jobs/total_jobs*100:.1f}%" if total_jobs > 0 else "0%")
    with col3:
        closed_statuses = ['COMPLETED', 'CLOSED', 'CANCELLED', 'REVIEW']
        closed_jobs = filtered_df[filtered_df['status'].isin(closed_statuses)].shape[0]
        st.metric(label="Closed Jobs", value=f"{closed_jobs:,}", delta=f"{closed_jobs/total_jobs*100:.1f}%" if total_jobs > 0 else "0%")
    
    today_jobs = filtered_df[filtered_df['createdAt'].dt.date == today]
    col1, col2 = st.columns(2)
    with col1:
        st.metric(label="Today's Jobs", value=f"{today_jobs.shape[0]:,}")
    with col2:
        today_closed = today_jobs[today_jobs['status'].isin(closed_statuses)].shape[0]
        st.metric(label="Closed Today's Jobs", value=f"{today_closed:,}")
    
    col1, col2 = st.columns(2)
    with col1:
        if 'status' in filtered_df.columns:
            status_counts = filtered_df['status'].value_counts().reset_index()
            status_counts.columns = ['Status', 'Count']
            colors = {'WAITINGJOB': '#FFA500', 'WORKING': '#1E90FF', 'PENDING': '#FFD700',
                      'COMPLETED': '#32CD32', 'CLOSED': '#006400', 'CANCELLED': '#DC143C'}
            status_fig = px.pie(status_counts, values='Count', names='Status', color='Status',
                                color_discrete_map=colors, title='Job Status Distribution', hole=0.4)
            status_fig.update_traces(textposition='inside', textinfo='percent+label')
            status_fig.update_layout(margin=dict(t=50, b=0, l=0, r=0),
                                     legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="center", x=0.5))
            st.plotly_chart(status_fig, use_container_width=True)
    with col2:
        if 'priority' in filtered_df.columns:
            priority_counts = filtered_df['priority'].value_counts().reset_index()
            priority_counts.columns = ['Priority', 'Count']
            priority_colors = {'HIGH': '#FF4500', 'MEDIUM': '#FFA500', 'LOW': '#32cd32'}
            priority_fig = px.bar(priority_counts, x='Priority', y='Count', color='Priority',
                                  color_discrete_map=priority_colors, title='Job Priority Distribution')
            priority_fig.update_layout(xaxis_title=None, yaxis_title="Number of Jobs", margin=dict(t=50, b=0, l=0, r=0))
            st.plotly_chart(priority_fig, use_container_width=True)
    
    if 'type' in filtered_df.columns:
        type_counts = filtered_df['type'].value_counts().reset_index()
        type_counts.columns = ['Type', 'Count']
        type_fig = px.bar(type_counts, x='Type', y='Count', title='Job Type Distribution')
        type_fig.update_layout(xaxis_title=None, yaxis_title="Number of Jobs", margin=dict(t=50, b=0, l=0, r=0))
        st.plotly_chart(type_fig, use_container_width=True)
    
    st.markdown("<h3>Recent Jobs</h3>", unsafe_allow_html=True)
    recent_cols = ['jobNo', 'status', 'type', 'priority', 'locationName', 'technician_names', 'createdAt']
    recent_jobs = filtered_df.sort_values('createdAt', ascending=False).head(10)
    display_df = recent_jobs[recent_cols].copy() if all(col in recent_jobs.columns for col in recent_cols) else recent_jobs
    if 'createdAt' in display_df.columns:
        display_df['createdAt'] = display_df['createdAt'].apply(format_date)
    def highlight_priority(val):
        if val == 'HIGH':
            return 'background-color: #ffebe6; color: #ff4b4b'
        elif val == 'MEDIUM':
            return 'background-color: #fff8e6; color: #ffa500'
        elif val == 'LOW':
            return 'background-color: #e6ffe6; color: #32cd32'
        return ''
    st.dataframe(display_df.style.applymap(highlight_priority, subset=['priority']), use_container_width=True)

with tab_overview:
    show_overview()

# ---------------------- Tab 2: Geographic Analysis ---------------------- #
def show_geographic_analysis():
    province_name_mapping = {
        "Mae Hong Son": "แม่ฮ่องสอน",
        "Chiang Mai": "เชียงใหม่",
        "Chiang Rai": "เชียงราย",
        # ... (other mappings remain unchanged)
        "Narathiwat": "นราธิวาส"
    }
    
    st.markdown("<h2 class='dashboard-title'>Geographic Analysis Dashboard</h2>", unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    
    with col1:
        if 'locationProvince' in filtered_df.columns:
            province_counts = filtered_df['locationProvince'].value_counts().reset_index()
            province_counts.columns = ['Province', 'Count']
            province_counts = province_counts.sort_values('Count', ascending=False).head(10)
            province_fig = px.bar(province_counts, x='Count', y='Province',
                                  title='Top 10 Provinces by Job Volume',
                                  orientation='h')
            province_fig.update_layout(xaxis_title="Number of Jobs", yaxis_title=None,
                                       yaxis={'categoryorder': 'total ascending'},
                                       margin=dict(t=50, b=0, l=0, r=0))
            st.plotly_chart(province_fig, use_container_width=True)
    
    with col2:
        if 'locationDistrict' in filtered_df.columns:
            district_counts = filtered_df['locationDistrict'].value_counts().reset_index()
            district_counts.columns = ['District', 'Count']
            district_counts = district_counts.sort_values('Count', ascending=False).head(10)
            district_fig = px.bar(district_counts, x='Count', y='District',
                                  title='Top 10 Districts by Job Volume',
                                  orientation='h')
            district_fig.update_layout(xaxis_title="Number of Jobs", yaxis_title=None,
                                       yaxis={'categoryorder': 'total ascending'},
                                       margin=dict(t=50, b=0, l=0, r=0))
            st.plotly_chart(district_fig, use_container_width=True)
    
    st.markdown("<h3>Job Distribution Map</h3>", unsafe_allow_html=True)
    m = folium.Map(location=[15.8700, 100.9925], zoom_start=6, tiles="CartoDB positron")
    has_coords = False
    if 'lat' in filtered_df.columns and 'lon' in filtered_df.columns:
        df_map = filtered_df.dropna(subset=['lat', 'lon'])
        has_coords = not df_map.empty
    
    if has_coords:
        marker_cluster = MarkerCluster().add_to(m)
        status_colors = {
            'WAITINGJOB': 'orange',
            'WORKING': 'blue',
            'PENDING': 'yellow',
            'COMPLETED': 'green',
            'CLOSED': 'darkgreen',
            'CANCELLED': 'red'
        }
        for _, row in df_map.iterrows():
            color = status_colors.get(row.get('status', ''), 'gray')
            popup_html = f"""
            <div style="font-family: Arial; width: 200px;">
                <b>Job No:</b> {row.get('jobNo', 'N/A')}<br>
                <b>Status:</b> {row.get('status', 'N/A')}<br>
                <b>Type:</b> {row.get('type', 'N/A')}<br>
                <b>Location:</b> {row.get('locationName', 'N/A')}<br>
                <b>Technician:</b> {row.get('technician_names', 'N/A')}<br>
                <b>Created:</b> {format_date(row.get('createdAt'))}
            </div>
            """
            folium.Marker(
                location=[row['lat'], row['lon']],
                popup=folium.Popup(popup_html, max_width=300),
                icon=folium.Icon(color=color, icon="wrench", prefix="fa")
            ).add_to(marker_cluster)
        legend_html = """
        <div style="position: fixed; bottom: 50px; left: 50px; z-index: 1000; background-color: white;
                     padding: 10px; border: 2px solid grey; border-radius: 5px;">
            <p><b>Job Status</b></p>
            <p><i class="fa fa-map-marker" style="color: orange;"></i> Waiting</p>
            <p><i class="fa fa-map-marker" style="color: blue;"></i> Working</p>
            <p><i class="fa fa-map-marker" style="color: yellow;"></i> Pending</p>
            <p><i class="fa fa-map-marker" style="color: green;"></i> Completed</p>
            <p><i class="fa fa-map-marker" style="color: darkgreen;"></i> Closed</p>
            <p><i class="fa fa-map-marker" style="color: red;"></i> Cancelled</p>
        </div>
        """
        m.get_root().html.add_child(folium.Element(legend_html))
    else:
        st.info("No precise coordinates available. Showing province-level summary.")
        province_coords = {
            "Bangkok": [13.7563, 100.5018],
            "Chiang Mai": [18.7883, 98.9853],
            "Phuket": [7.9519, 98.3381],
            "Chon Buri": [13.3611, 100.9847],
            "Songkhla": [7.1756, 100.6142],
            "Nakhon Ratchasima": [14.9801, 102.0978],
            "Khon Kaen": [16.4419, 102.8360],
            "Rayong": [12.6831, 101.2376],
            "Udon Thani": [17.4138, 102.7870],
            "Chiang Rai": [19.9105, 99.8406]
        }
        province_counts = filtered_df['locationProvince'].value_counts().reset_index()
        province_counts.columns = ['Province', 'Count']
        for province, count in zip(province_counts['Province'], province_counts['Count']):
            if province in province_coords:
                size = int(8 + (count / province_counts['Count'].max() * 10))
                popup_text = f"{province}: {count} jobs"
                folium.CircleMarker(
                    location=province_coords[province],
                    radius=count / province_counts['Count'].max() * 30 + 5,
                    popup=popup_text,
                    color='blue',
                    fill=True,
                    fill_color='blue',
                    fill_opacity=0.6
                ).add_to(m)
                folium.Marker(
                    location=province_coords[province],
                    icon=folium.DivIcon(
                        icon_size=(150,36),
                        icon_anchor=(75,18),
                        html=f'<div style="font-size: {size}px; font-weight: bold; text-align: center;">{province}</div>'
                    )
                ).add_to(m)
    folium_static(m, width=1100, height=500)
    
    st.markdown("<h3>Geographic Heat Map by Job Status</h3>", unsafe_allow_html=True)
    if 'locationProvince' in filtered_df.columns:
        province_status = filtered_df.groupby(['locationProvince', 'status']).size().reset_index(name='count')
        province_status_pivot = province_status.pivot_table(index='locationProvince',
                                                            columns='status',
                                                            values='count',
                                                            fill_value=0).reset_index()
        province_status_pivot['Total'] = province_status_pivot.iloc[:, 1:].sum(axis=1)
        heatmap_cols = list(province_status_pivot.columns[1:-1])
        heatmap_fig = px.imshow(province_status_pivot[heatmap_cols].values,
                                x=heatmap_cols,
                                y=province_status_pivot['locationProvince'],
                                color_continuous_scale='Viridis',
                                title='Job Distribution by Province and Status',
                                labels=dict(x="Status", y="Province", color="Count"))
        heatmap_fig.update_layout(margin=dict(t=50, b=0, l=0, r=0), height=600)
        st.plotly_chart(heatmap_fig, use_container_width=True)

with tab_geo:
    show_geographic_analysis()

# ---------------------- Tab 3: Technician Performance ---------------------- #
def show_technician_performance():
    st.markdown("<h2 class='dashboard-title'>Technician Performance Dashboard</h2>", unsafe_allow_html=True)   
    st.markdown("<h3>Latest Technician Reviews</h3>", unsafe_allow_html=True)
    if not df_reviews.empty:
        review_display = df_reviews.sort_values('createdAt', ascending=False).head(10)
        if 'createdAt' in review_display.columns:
            review_display['createdAt'] = review_display['createdAt'].apply(format_date)
        display_cols = ['technician_name', 'jobNo', 'time', 'manner', 'knowledge', 'overall', 'recommend', 'createdAt']
        st.dataframe(review_display[display_cols], use_container_width=True)
    else:
        st.info("No review data available for the selected period.")
    
    st.markdown("<h3>Overall Technician Review Summary</h3>", unsafe_allow_html=True)
    if not df_reviews.empty:
        review_summary = df_reviews.groupby('technician_name').agg({
            'time': 'mean',
            'manner': 'mean',
            'knowledge': 'mean',
            'overall': 'mean',
            'recommend': 'mean'
        }).reset_index()
        review_melt = review_summary.melt(id_vars=['technician_name'], 
                                          value_vars=['time', 'manner', 'knowledge', 'overall', 'recommend'],
                                          var_name='Metric', value_name='Average')
        review_fig = px.bar(review_melt, x='technician_name', y='Average', color='Metric',
                            barmode='group', title='Average Review Scores by Technician')
        review_fig.update_layout(xaxis_title="Technician", yaxis_title="Average Score",
                                 margin=dict(t=50, b=0, l=0, r=0))
        st.plotly_chart(review_fig, use_container_width=True)
    else:
        st.info("No review data available for summary analysis.")

with tab_tech:
    show_technician_performance()

# ---------------------- Footer ---------------------- #
st.markdown("---")
st.markdown(
    """
    <div style="text-align: center; color: #888888; font-size: 12px;">
        Job Management Dashboard © 2025 | Created by MIS | Data updates every minute
    </div>
    """,
    unsafe_allow_html=True
)
