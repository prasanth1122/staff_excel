import streamlit as st
import pandas as pd
from io import BytesIO
import re
from rapidfuzz import fuzz, process
from xlsxwriter.utility import xl_col_to_name
import math
from datetime import datetime
import openpyxl

st.title("📊 Staff Campaign + Shopify Data Processor with Date Columns")
st.markdown("**Staff version with multiple file uploads and date-based column grouping for score calculation!**")

# ---- MULTIPLE FILE UPLOADS ----
st.subheader("📁 Upload Campaign Data Files")
campaign_files = st.file_uploader(
    "Upload Campaign Data Files (Excel/CSV)", 
    type=["xlsx", "csv"], 
    accept_multiple_files=True,
    key="campaign_files",
    help="Upload one or more Facebook Ads campaign files. Files with matching products and campaign names will be merged."
)

st.subheader("🛒 Upload Shopify Data Files")
shopify_files = st.file_uploader(
    "Upload Shopify Data Files (Excel/CSV)", 
    type=["xlsx", "csv"], 
    accept_multiple_files=True,
    key="shopify_files",
    help="Upload one or more Shopify sales files. Files with matching products and variants will be merged."
)

st.subheader("📋 Upload Reference Data Files (Optional)")
old_merged_files = st.file_uploader(
    "Upload Reference Data Files (Excel/CSV) - to import delivery rates and product costs",
    type=["xlsx", "csv"],
    accept_multiple_files=True,
    key="reference_files",
    help="Upload one or more previous merged data files to automatically import delivery rates and product costs for matching products"
)


# ---- HELPERS ----
def safe_write(worksheet, row, col, value, cell_format=None):
    """Wrapper around worksheet.write to handle NaN/inf safely"""
    if isinstance(value, (int, float)):
        if value is None or (isinstance(value, float) and (math.isnan(value) or math.isinf(value))):
            value = 0
    else:
        if pd.isna(value):
            value = ""
    worksheet.write(row, col, value, cell_format)

def read_file(file):
    """Helper function to read uploaded file"""
    try:
        if file.name.endswith(".csv"):
            return pd.read_csv(file)
        else:
            return pd.read_excel(file)
    except Exception as e:
        st.error(f"❌ Error reading file {file.name}: {str(e)}")
        return None

def find_date_column(df):
    """Find date column in dataframe"""
    date_columns = []
    for col in df.columns:
        if any(keyword in col.lower() for keyword in ['day', 'date', 'time']):
            date_columns.append(col)
    return date_columns[0] if date_columns else None

def standardize_campaign_columns(df):
    """Standardize campaign column names and handle currency conversion"""
    df = df.copy()
    
    # Find and preserve original date column
    date_col = find_date_column(df)
    if date_col:
        df['Date'] = df[date_col]
        if date_col != 'Date':
            df = df.drop(columns=[date_col])
        st.info(f"📅 Found date column: {date_col}")
    # Find and preserve Delivery status column
    delivery_status_col = None
    for col in df.columns:
        if 'delivery' in col.lower() and 'status' in col.lower():
            delivery_status_col = col
            break
    
    if delivery_status_col:
        df['Delivery status'] = df[delivery_status_col]
        if delivery_status_col != 'Delivery status':
            df = df.drop(columns=[delivery_status_col])
        st.info(f"📝 Found Delivery status column: {delivery_status_col}")
    # Find purchases/results column
    purchases_col = None
    for col in df.columns:
        if col.lower() in ['purchases', 'results']:
            purchases_col = col
            break
    
    if purchases_col and purchases_col != 'Purchases':
        df = df.rename(columns={purchases_col: 'Purchases'})
        st.info(f"📝 Renamed '{purchases_col}' to 'Purchases'")
    
    # Find amount spent column and handle currency
    amount_col = None
    is_inr = False
    
    # Check for USD first
    for col in df.columns:
        if 'amount spent' in col.lower() and 'usd' in col.lower():
            amount_col = col
            is_inr = False
            break
    
    # If no USD found, check for INR
    if not amount_col:
        for col in df.columns:
            if 'amount spent' in col.lower() and 'inr' in col.lower():
                amount_col = col
                is_inr = True
                break
    
    # If neither USD nor INR specified, assume it's INR and convert
    if not amount_col:
        for col in df.columns:
            if 'amount spent' in col.lower():
                amount_col = col
                is_inr = True  # Assume INR if currency not specified
                break
    
    if amount_col:
        if is_inr:
            # Convert INR to USD by dividing by 100
            df['Amount spent (USD)'] = df[amount_col] / 100
            st.info(f"💱 Converted '{amount_col}' from INR to USD (divided by 100)")
        else:
            df['Amount spent (USD)'] = df[amount_col]
            if amount_col != 'Amount spent (USD)':
                st.info(f"📝 Renamed '{amount_col}' to 'Amount spent (USD)'")
        
        # Remove original column if it's different
        if amount_col != 'Amount spent (USD)':
            df = df.drop(columns=[amount_col])
    
    return df

def merge_campaign_files(files):
    """Merge multiple campaign files"""
    if not files:
        return None
    
    all_campaigns = []
    file_info = []
    
    for file in files:
        df = read_file(file)
        if df is not None:
            # Standardize columns and handle currency conversion
            df = standardize_campaign_columns(df)
            all_campaigns.append(df)
            file_info.append(f"{file.name} ({len(df)} rows)")
    
    if not all_campaigns:
        return None
    
    # Combine all campaign files
    merged_df = pd.concat(all_campaigns, ignore_index=True)
    
    # Group by Campaign name and Date (if available) and sum amounts
    group_cols = ["Campaign name"]
    if 'Date' in merged_df.columns:
        group_cols.append('Date')
    
    required_cols = group_cols + ["Amount spent (USD)"]
    if all(col in merged_df.columns for col in required_cols):
        # Check if Purchases column exists
        has_purchases = "Purchases" in merged_df.columns
        has_delivery_status = "Delivery status" in merged_df.columns
        agg_dict = {"Amount spent (USD)": "sum"}
        if has_purchases:
            agg_dict["Purchases"] = "sum"
        if has_delivery_status:
            agg_dict["Delivery status"] = "first"  # Keep first delivery status value
        merged_df = merged_df.groupby(group_cols, as_index=False).agg(agg_dict)
    
    st.success(f"✅ Successfully merged {len(files)} campaign files:")
    for info in file_info:
        st.write(f"  • {info}")
    st.write(f"**Total campaigns after merging: {len(merged_df)}**")
    
    return merged_df

def merge_shopify_files(files):
    """Merge multiple Shopify files"""
    if not files:
        return None
    
    all_shopify = []
    file_info = []
    
    for file in files:
        df = read_file(file)
        if df is not None:
            # Find and preserve original date column
            date_col = find_date_column(df)
            if date_col:
                df['Date'] = df[date_col]
                if date_col != 'Date':
                    df = df.drop(columns=[date_col])
                st.info(f"📅 Found Shopify date column: {date_col}")
            
            all_shopify.append(df)
            file_info.append(f"{file.name} ({len(df)} rows)")
    
    if not all_shopify:
        return None
    
    # Combine all Shopify files
    merged_df = pd.concat(all_shopify, ignore_index=True)
    
    # Group by Product title + Product variant title + Date (if available)
    group_cols = ["Product title", "Product variant title"]
    if 'Date' in merged_df.columns:
        group_cols.append('Date')
    
    required_cols = group_cols + ["Net items sold"]
    if all(col in merged_df.columns for col in required_cols):
        # Group and sum net items sold, keep first price
        agg_dict = {"Net items sold": "sum"}
        if "Product variant price" in merged_df.columns:
            agg_dict["Product variant price"] = "first"  # Keep first price found
        
        merged_df = merged_df.groupby(group_cols, as_index=False).agg(agg_dict)
    
    st.success(f"✅ Successfully merged {len(files)} Shopify files:")
    for info in file_info:
        st.write(f"  • {info}")
    st.write(f"**Total product variants after merging: {len(merged_df)}**")
    
    return merged_df

def merge_reference_files(files):
    """Merge multiple reference files for delivery rates and product costs"""
    if not files:
        return None
    
    all_references = []
    file_info = []
    
    for file in files:
        df = read_file(file)
        if df is not None:
            required_old_cols = ["Product title", "Product variant title", "Delivery Rate"]
            if all(col in df.columns for col in required_old_cols):
                # Process the reference file similar to original logic
                current_product = None
                for idx, row in df.iterrows():
                    if pd.notna(row["Product title"]) and row["Product title"].strip() != "":
                        if row["Product variant title"] == "ALL VARIANTS (TOTAL)":
                            current_product = row["Product title"]
                        else:
                            current_product = row["Product title"]
                    else:
                        if current_product:
                            df.loc[idx, "Product title"] = current_product

                # Filter out total rows
                df_filtered = df[
                    (df["Product variant title"] != "ALL VARIANTS (TOTAL)") &
                    (df["Product variant title"] != "ALL PRODUCTS") &
                    (df["Delivery Rate"].notna()) & (df["Delivery Rate"] != "")
                ]
                
                if not df_filtered.empty:
                    df_filtered["Product title_norm"] = df_filtered["Product title"].astype(str).str.strip().str.lower()
                    df_filtered["Product variant title_norm"] = df_filtered["Product variant title"].astype(str).str.strip().str.lower()
                    all_references.append(df_filtered)
                    file_info.append(f"{file.name} ({len(df_filtered)} valid records)")
            else:
                st.warning(f"⚠️ Reference file {file.name} doesn't contain required columns")
    
    if not all_references:
        return None
    
    # Combine all reference files
    merged_df = pd.concat(all_references, ignore_index=True)
    
    # For duplicates, keep the last occurrence (latest file takes priority)
    merged_df = merged_df.drop_duplicates(
        subset=["Product title_norm", "Product variant title_norm"], 
        keep="last"
    )
    
    has_product_cost = "Product Cost (Input)" in merged_df.columns
    st.success(f"✅ Successfully merged {len(files)} reference files:")
    for info in file_info:
        st.write(f"  • {info}")
    st.write(f"**Total unique delivery rate records: {len(merged_df)}**")
    
    if has_product_cost:
        product_cost_count = merged_df["Product Cost (Input)"].notna().sum()
        st.write(f"**Product cost records found: {product_cost_count}**")
    
    return merged_df

# ---- STATE ----
df_campaign, df_shopify, df_old_merged = None, None, None
grouped_campaign = None

# Initialize day-wise lookup dictionaries
product_date_avg_prices = {}
product_date_delivery_rates = {}
product_date_cost_inputs = {}




# ---- PROCESS MULTIPLE REFERENCE FILES ----
if old_merged_files:
    df_old_merged = merge_reference_files(old_merged_files)
    
    if df_old_merged is not None:
        has_product_cost = "Product Cost (Input)" in df_old_merged.columns
        
        # Show preview
        preview_cols = ["Product title", "Product variant title", "Delivery Rate"]
        if has_product_cost:
            preview_cols.append("Product Cost (Input)")
        st.write("**Preview of merged reference data:**")
        st.write(df_old_merged[preview_cols].head(10))

# ---- PROCESS MULTIPLE CAMPAIGN FILES ----
if campaign_files:
    df_campaign = merge_campaign_files(campaign_files)
    
    if df_campaign is not None:
        st.subheader("📂 Merged Campaign Data")
        st.write(df_campaign)

        # ---- CLEAN PRODUCT NAME ----
        def clean_product_name(name: str) -> str:
            text = str(name).strip()
            match = re.split(r"[-/|]|\s[xX]\s", text, maxsplit=1)
            base = match[0] if match else text
            base = base.lower()
            base = re.sub(r'[^a-z0-9 ]', '', base)
            base = re.sub(r'\s+', ' ', base)
            return base.strip().title()

        df_campaign["Product Name"] = df_campaign["Campaign name"].astype(str).apply(clean_product_name)

        # ---- FUZZY DEDUP ----
        unique_names = df_campaign["Product Name"].unique().tolist()
        mapping = {}
        for name in unique_names:
            if name in mapping:
                continue
            result = process.extractOne(name, mapping.keys(), scorer=fuzz.token_sort_ratio, score_cutoff=85)
            if result:
                mapping[name] = mapping[result[0]]
            else:
                mapping[name] = name
        df_campaign["Canonical Product"] = df_campaign["Product Name"].map(mapping)

        # ---- GROUP BY CANONICAL PRODUCT (without date for summary) ----
        grouped_campaign = (
            df_campaign.groupby("Canonical Product", as_index=False)
            .agg({"Amount spent (USD)": "sum"})
        )
        grouped_campaign["Amount spent (INR)"] = grouped_campaign["Amount spent (USD)"] * 100
        grouped_campaign = grouped_campaign.rename(columns={
            "Canonical Product": "Product",
            "Amount spent (USD)": "Total Amount Spent (USD)",
            "Amount spent (INR)": "Total Amount Spent (INR)"
        })

        st.subheader("✅ Processed Campaign Data")
        st.write(grouped_campaign)

        # ---- FINAL CAMPAIGN DATA STRUCTURE WITH DATE GROUPING ----
        final_campaign_data = []
        has_purchases = "Purchases" in df_campaign.columns
        has_dates = 'Date' in df_campaign.columns
        has_delivery_status = 'Delivery status' in df_campaign.columns

        for product, product_campaigns in df_campaign.groupby("Canonical Product"):
            for _, campaign in product_campaigns.iterrows():
                row = {
                    "Product Name": "",
                    "Campaign Name": campaign["Campaign name"],
                    "Amount Spent (USD)": campaign["Amount spent (USD)"],
                    "Amount Spent (INR)": campaign["Amount spent (USD)"] * 100,
                    "Product": product
                }
                if has_purchases:
                    row["Purchases"] = campaign.get("Purchases", 0)
                if has_dates:
                    row["Date"] = campaign.get("Date", "")
                if has_delivery_status:
                    row["Delivery status"] = campaign.get("Delivery status", "")
                final_campaign_data.append(row)

        df_final_campaign = pd.DataFrame(final_campaign_data)

        if not df_final_campaign.empty:
            # Sort by product spending and then by date
            order = (
                df_final_campaign.groupby("Product")["Amount Spent (INR)"].sum().sort_values(ascending=False).index
            )
            df_final_campaign["Product"] = pd.Categorical(df_final_campaign["Product"], categories=order, ordered=True)
            
            sort_cols = ["Product"]
            if has_dates:
                sort_cols.append("Date")
            
            df_final_campaign = df_final_campaign.sort_values(sort_cols).reset_index(drop=True)
            df_final_campaign["Delivered Orders"] = ""
            df_final_campaign["Delivery Rate"] = ""

        st.subheader("🎯 Final Campaign Data Structure with Date Grouping")
        display_cols = [col for col in df_final_campaign.columns if col != "Product"]
        st.write(df_final_campaign[display_cols])

# ---- PROCESS MULTIPLE SHOPIFY FILES ----
if shopify_files:
    df_shopify = merge_shopify_files(shopify_files)
    
    if df_shopify is not None:
        required_cols = ["Product title", "Product variant title", "Product variant price", "Net items sold"]
        available_cols = [col for col in required_cols if col in df_shopify.columns]
        
        # Keep date columns if they exist
        if 'Date' in df_shopify.columns:
            available_cols.append('Date')
            
        df_shopify = df_shopify[available_cols]

        # Add extra columns for staff (simplified)
        df_shopify["Product Cost (Input)"] = ""
        df_shopify["Delivery Rate"] = ""
        df_shopify["Ad Spend (USD)"] = 0.0

        # ---- IMPORT DELIVERY RATES AND PRODUCT COSTS FROM MERGED REFERENCE DATA ----
        if df_old_merged is not None:
            # Create normalized versions for matching (case insensitive)
            df_shopify["Product title_norm"] = df_shopify["Product title"].astype(str).str.strip().str.lower()
            df_shopify["Product variant title_norm"] = df_shopify["Product variant title"].astype(str).str.strip().str.lower()
            
            # Create lookup dictionaries from old data
            delivery_rate_lookup = {}
            product_cost_lookup = {}
            has_product_cost = "Product Cost (Input)" in df_old_merged.columns
            
            for _, row in df_old_merged.iterrows():
                key = (row["Product title_norm"], row["Product variant title_norm"])
                
                # Store delivery rate
                delivery_rate_lookup[key] = row["Delivery Rate"]
                
                # Store product cost if column exists and has value
                if has_product_cost and pd.notna(row["Product Cost (Input)"]) and row["Product Cost (Input)"] != "":
                    product_cost_lookup[key] = row["Product Cost (Input)"]
            
            # Match and update delivery rates and product costs
            delivery_matched_count = 0
            product_cost_matched_count = 0
            
            for idx, row in df_shopify.iterrows():
                key = (row["Product title_norm"], row["Product variant title_norm"])
                
                # Update delivery rate
                if key in delivery_rate_lookup:
                    df_shopify.loc[idx, "Delivery Rate"] = delivery_rate_lookup[key]
                    delivery_matched_count += 1
                
                # Update product cost
                if key in product_cost_lookup:
                    df_shopify.loc[idx, "Product Cost (Input)"] = product_cost_lookup[key]
                    product_cost_matched_count += 1
            
            # Clean up temporary normalized columns
            df_shopify = df_shopify.drop(columns=["Product title_norm", "Product variant title_norm"])
            
            st.success(f"✅ Successfully imported delivery rates for {delivery_matched_count} product variants from reference data")
            if has_product_cost and product_cost_matched_count > 0:
                st.success(f"✅ Successfully imported product costs for {product_cost_matched_count} product variants from reference data")
            elif has_product_cost:
                st.info("ℹ️ No product cost matches found in reference data")

        # ---- CLEAN SHOPIFY PRODUCT TITLES TO MATCH CAMPAIGN ----
        def clean_product_name(name: str) -> str:
            text = str(name).strip()
            match = re.split(r"[-/|]|\s[xX]\s", text, maxsplit=1)
            base = match[0] if match else text
            base = base.lower()
            base = re.sub(r'[^a-z0-9 ]', '', base)
            base = re.sub(r'\s+', ' ', base)
            return base.strip().title()

        df_shopify["Product Name"] = df_shopify["Product title"].astype(str).apply(clean_product_name)

        # Build candidate set from campaign canonical names
        campaign_products = grouped_campaign["Product"].unique().tolist() if grouped_campaign is not None else []

        def fuzzy_match_to_campaign(name, choices, cutoff=85):
            if not choices:
                return name
            result = process.extractOne(name, choices, scorer=fuzz.token_sort_ratio, score_cutoff=cutoff)
            return result[0] if result else name

        # Apply fuzzy matching for Shopify → Campaign
        df_shopify["Canonical Product"] = df_shopify["Product Name"].apply(
            lambda x: fuzzy_match_to_campaign(x, campaign_products)
        )

        # ---- CORRECTED AD SPEND ALLOCATION (DAY-WISE DISTRIBUTION) ----
        if grouped_campaign is not None and df_campaign is not None:
            # Initialize Ad Spend to 0 for all rows
            df_shopify["Ad Spend (USD)"] = 0.0
            
            # Create campaign spend lookup by product and date
            campaign_spend_by_product_date = {}
            
            # First, build the campaign spend lookup from df_campaign (which has dates)
            if 'Date' in df_campaign.columns:
                for _, row in df_campaign.iterrows():
                    product = row['Canonical Product']
                    date = str(row['Date'])
                    amount_usd = row['Amount spent (USD)']
                    
                    if product not in campaign_spend_by_product_date:
                        campaign_spend_by_product_date[product] = {}
                    
                    if date not in campaign_spend_by_product_date[product]:
                        campaign_spend_by_product_date[product][date] = 0
                    
                    campaign_spend_by_product_date[product][date] += amount_usd
            
            # FIXED: Track which products have received date-specific allocation
            products_with_date_allocation = set()
            
            # Now allocate ad spend to Shopify variants based on their share of items sold per product per date
            for product, product_df in df_shopify.groupby("Canonical Product"):
                if product in campaign_spend_by_product_date:
                    has_any_date_allocation = False
                    
                    # For each date, calculate total items sold by this product on that date
                    for date in campaign_spend_by_product_date[product].keys():
                        date_campaign_spend = campaign_spend_by_product_date[product][date]
                        
                        # Get all variants of this product sold on this date
                        product_date_variants = product_df[product_df['Date'].astype(str) == date]
                        
                        if not product_date_variants.empty:
                            total_items_on_date = product_date_variants['Net items sold'].sum()
                            
                            if total_items_on_date > 0:
                                # Distribute the campaign spend for this date proportionally
                                for idx, variant_row in product_date_variants.iterrows():
                                    variant_items = variant_row['Net items sold']
                                    variant_share = variant_items / total_items_on_date
                                    variant_ad_spend = date_campaign_spend * variant_share
                                    
                                    # Update the ad spend for this specific variant on this date
                                    df_shopify.loc[idx, "Ad Spend (USD)"] = variant_ad_spend
                                    has_any_date_allocation = True
                    
                    # FIXED: Mark this product as having received date-specific allocation
                    if has_any_date_allocation:
                        products_with_date_allocation.add(product)
            
            # For products without date-specific campaign data, fall back to total allocation
            ad_spend_map = dict(zip(grouped_campaign["Product"], grouped_campaign["Total Amount Spent (USD)"]))
            
            for product, product_df in df_shopify.groupby("Canonical Product"):
                # FIXED: Only allocate total spend if this product did NOT get date-specific allocation
                if product not in products_with_date_allocation and product in ad_spend_map:
                    total_items = product_df["Net items sold"].sum()
                    if total_items > 0:
                        total_spend_usd = ad_spend_map[product]
                        
                        # Allocate spend based on items sold
                        for idx, variant_row in product_df.iterrows():
                            variant_items = variant_row['Net items sold']
                            variant_share = variant_items / total_items
                            df_shopify.loc[idx, "Ad Spend (USD)"] = total_spend_usd * variant_share

        # ---- CREATE DAY-WISE LOOKUPS FROM SHOPIFY DATA ----
        if df_shopify is not None and not df_shopify.empty and 'Date' in df_shopify.columns:
            st.subheader("🔍 Creating Day-wise Lookups from Shopify Data")
            
            # Get unique dates
            unique_dates = sorted([str(d) for d in df_shopify['Date'].unique() if pd.notna(d) and str(d).strip() != ''])
            
            # Initialize lookups for all products and dates
            for product in df_shopify['Canonical Product'].unique():
                product_date_avg_prices[product] = {}
                product_date_delivery_rates[product] = {}
                product_date_cost_inputs[product] = {}
                
                for date in unique_dates:
                    product_date_avg_prices[product][date] = 0
                    product_date_delivery_rates[product][date] = 0
                    product_date_cost_inputs[product][date] = 0
            
            # Build lookups from Shopify data
            for product, product_df in df_shopify.groupby('Canonical Product'):
                for date in unique_dates:
                    # Filter data for this product and date
                    date_data = product_df[product_df['Date'].astype(str) == date]
                    
                    if not date_data.empty:
                        # Calculate weighted averages for this product-date combination
                        total_net_items = date_data['Net items sold'].sum()
                        
                        if total_net_items > 0:
                            # Weighted average price
                            total_revenue = (date_data['Product variant price'] * date_data['Net items sold']).sum()
                            avg_price = total_revenue / total_net_items
                            product_date_avg_prices[product][date] = avg_price
                            
                            # Weighted average delivery rate
                            delivery_rates = []
                            cost_inputs = []
                            
                            for _, row in date_data.iterrows():
                                net_items = row['Net items sold']
                                delivery_rate = row.get('Delivery Rate', 0)
                                cost_input = row.get('Product Cost (Input)', 0)
                                
                                # Convert delivery rate if it's a string percentage
                                if isinstance(delivery_rate, str):
                                    delivery_rate = delivery_rate.strip().replace('%', '')
                                delivery_rate = pd.to_numeric(delivery_rate, errors='coerce') or 0
                                if delivery_rate > 1:  # assume it's given as percentage
                                    delivery_rate = delivery_rate / 100.0
                                
                                cost_input = pd.to_numeric(cost_input, errors='coerce') or 0
                                
                                if net_items > 0:
                                    delivery_rates.extend([delivery_rate] * int(net_items))
                                    cost_inputs.extend([cost_input] * int(net_items))
                            
                            # Calculate weighted averages
                            if delivery_rates:
                                product_date_delivery_rates[product][date] = sum(delivery_rates) / len(delivery_rates)
                            
                            if cost_inputs:
                                product_date_cost_inputs[product][date] = sum(cost_inputs) / len(cost_inputs)
            
            # Display lookup summary
            st.success("✅ Day-wise lookups created successfully!")
            
            # Show sample of lookups
            sample_products = list(product_date_avg_prices.keys())[:3]  # Show first 3 products
            for product in sample_products:
                st.write(f"**{product}:**")
                for date in unique_dates[:3]:  # Show first 3 dates
                    avg_price = product_date_avg_prices[product].get(date, 0)
                    delivery_rate = product_date_delivery_rates[product].get(date, 0)
                    cost_input = product_date_cost_inputs[product].get(date, 0)
                    
                    if avg_price > 0 or delivery_rate > 0 or cost_input > 0:
                        st.write(f"  • {date}: Price=${avg_price:.2f}, Rate={delivery_rate:.2%}, Cost=${cost_input:.2f}")

        # ---- SORT PRODUCTS BY NET ITEMS SOLD (DESC) ----
        product_order = (
            df_shopify.groupby("Product title")["Net items sold"]
            .sum()
            .sort_values(ascending=False)
            .index
        )

        df_shopify["Product title"] = pd.Categorical(df_shopify["Product title"], categories=product_order, ordered=True)
        
        # Sort by product, then by date if available
        sort_cols = ["Product title"]
        if 'Date' in df_shopify.columns:
            sort_cols.append("Date")
            
        df_shopify = df_shopify.sort_values(by=sort_cols).reset_index(drop=True)

        st.subheader("🛒 Merged Shopify Data with CORRECTED Ad Spend (USD) & Date Grouping")
        
        # Show delivery rate and product cost import summary
        if df_old_merged is not None:
            delivery_rate_filled = df_shopify["Delivery Rate"].astype(str).str.strip()
            delivery_rate_filled = delivery_rate_filled[delivery_rate_filled != ""]
            
            product_cost_filled = df_shopify["Product Cost (Input)"].astype(str).str.strip()
            product_cost_filled = product_cost_filled[product_cost_filled != ""]
            
            st.info(f"📊 Delivery rates imported: {len(delivery_rate_filled)} out of {len(df_shopify)} variants")
            if len(product_cost_filled) > 0:
                st.info(f"📊 Product costs imported: {len(product_cost_filled)} out of {len(df_shopify)} variants")
        
        # Show date information
        has_shopify_dates = 'Date' in df_shopify.columns
        if has_shopify_dates:
            unique_dates = df_shopify['Date'].unique()
            unique_dates = [str(d) for d in unique_dates if pd.notna(d) and str(d).strip() != '']
            st.info(f"📅 Found {len(unique_dates)} unique dates in Shopify data: {', '.join(sorted(unique_dates)[:5])}{'...' if len(unique_dates) > 5 else ''}")
        
        # Show ad spend verification
        total_shopify_ad_spend = df_shopify["Ad Spend (USD)"].sum()
        total_campaign_spend = grouped_campaign["Total Amount Spent (USD)"].sum() if grouped_campaign is not None else 0
        st.info(f"💰 Ad Spend Verification: Shopify Total = ${total_shopify_ad_spend:.2f}, Campaign Total = ${total_campaign_spend:.2f}")
        
        # Display without internal columns
        display_cols = [col for col in df_shopify.columns if col not in ["Product Name", "Canonical Product"]]
        st.write(df_shopify[display_cols])


# ---- BUILD SHOPIFY TOTALS LOOKUP ----
shopify_totals = {}

if df_shopify is not None and not df_shopify.empty:
    for product, product_df in df_shopify.groupby("Canonical Product"):
        delivered_orders = 0
        total_sold = 0

        for _, row in product_df.iterrows():
            rate = row.get("Delivery Rate", "")
            sold = pd.to_numeric(row.get("Net items sold", 0), errors="coerce") or 0

            # Clean rate (it might be "70%" or 0.7 or 70)
            if isinstance(rate, str):
                rate = rate.strip().replace("%", "")
            rate = pd.to_numeric(rate, errors="coerce")
            if pd.isna(rate):
                rate = 0
            if rate > 1:  # assume it's given as percentage
                rate = rate / 100.0

            delivered_orders += sold * rate
            total_sold += sold

        delivery_rate = delivered_orders / total_sold if total_sold > 0 else 0

        shopify_totals[product] = {
            "Delivered Orders": round(delivered_orders, 1),
            "Delivery Rate": delivery_rate
        }

# ---- BUILD WEIGHTED AVERAGE LOOKUPS ----
avg_price_lookup = {}
if df_shopify is not None and not df_shopify.empty:
    for product, product_df in df_shopify.groupby("Canonical Product"):
        total_sold = product_df["Net items sold"].sum()
        if total_sold > 0:
            weighted_avg_price = (
                (product_df["Product variant price"] * product_df["Net items sold"]).sum()
                / total_sold
            )
            avg_price_lookup[product] = weighted_avg_price

avg_product_cost_lookup = {}
if df_shopify is not None and not df_shopify.empty:
    for product, product_df in df_shopify.groupby("Canonical Product"):
        total_sold = product_df["Net items sold"].sum()
        valid_df = product_df[pd.to_numeric(product_df["Product Cost (Input)"], errors="coerce").notna()]
        if total_sold > 0 and not valid_df.empty:
            weighted_avg_cost = (
                (pd.to_numeric(valid_df["Product Cost (Input)"], errors="coerce") * valid_df["Net items sold"]).sum()
                / valid_df["Net items sold"].sum()
            )
            avg_product_cost_lookup[product] = weighted_avg_cost

unique_campaign_dates = []
if campaign_files and df_campaign is not None and 'Date' in df_campaign.columns:
    unique_campaign_dates = sorted([str(d) for d in df_campaign['Date'].unique() if pd.notna(d) and str(d).strip() != ''])

# Calculate default value based on number of unique dates
if len(unique_campaign_dates) > 0:
    n_days = len(unique_campaign_dates)
    if n_days % 2 == 0:
        default_days = n_days // 2  # Even: n/2
    else:
        default_days = (n_days + 1) // 2  # Odd: (n+1)/2
    
    st.info(f"📅 Found {n_days} unique dates in campaign data")
    
    # Input slider for selecting number of days
    selected_days = st.slider(
        "Select number of days to check for negative scores (random, not consecutive)",
        min_value=1,
        max_value=n_days,
        value=default_days,
        help=f"Default is {default_days} days (n/2 for even or (n+1)/2 for odd number of total days). "
             f"The analysis will check if campaigns have negative scores in this many days randomly distributed across all dates."
    )
    
    st.write(f"**Analysis will check:** {selected_days} out of {n_days} total days for negative scores (random distribution)")
else:
    selected_days = 1  # Default fallback
    st.warning("⚠️ No campaign dates found. Using default value of 1 day.")



def convert_shopify_to_excel_staff_with_date_columns_corrected(df, campaign_df=None):
    """Convert Shopify data to Excel for staff with date columns and CORRECTED ad spend distribution"""
    if df is None or df.empty:
        return None
        
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet("Shopify Staff")
        writer.sheets["Shopify Staff"] = worksheet

        # Staff-specific formats (with two decimal places)
        header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#BDD7EE", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        date_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#B4C6E7", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        total_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#FFD966", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        grand_total_format = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#EEEE0E", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        
       
        
        # Get unique dates and sort them
        unique_dates = sorted([str(d) for d in df['Date'].unique() if pd.notna(d) and str(d).strip() != ''])
        num_days = len(unique_dates)
        
        # Calculate dynamic threshold
        dynamic_threshold = num_days * 1
        
        # Dynamic conditional formats based on calculated threshold (with two decimal places)
        product_total_format_low = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#DC4E23", "font_name": "Calibri", "font_size": 11,  # Red
            "num_format": "#,##0.00"
        })
        variant_format_low = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#FFCCCB", "font_name": "Calibri", "font_size": 11,  # Light red
            "num_format": "#,##0.00"
        })
        
        product_total_format_high = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#FFD966", "font_name": "Calibri", "font_size": 11,  # Default
            "num_format": "#,##0.00"
        })
        variant_format_high = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#D9E1F2", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })

        # Define base columns for staff
        base_columns = ["Product title", "Product variant title", "Delivery Rate", "Product Cost (Input)", "Total Net items sold", "Amount Spent", "C.P.I", "B.E"]
        
        # Define staff metrics (simplified - only 6 metrics per date)
        date_metrics = ["Net items sold", "Avg Price", "Ad Spend (USD)", "Delivery Rate", "Product Cost Input", "Score"]
        
        # Build column structure WITH SEPARATOR COLUMNS
        all_columns = base_columns.copy()
        all_columns.append("SEPARATOR_AFTER_BASE")
        
        # Add date-specific columns with separators
        for date in unique_dates:
            for metric in date_metrics:
                all_columns.append(f"{date}_{metric}")
            all_columns.append(f"SEPARATOR_AFTER_{date}")
        
        # Add total columns
        for metric in date_metrics:
            all_columns.append(f"Total_{metric}")

        # Write headers (skip separator columns)
        for col_num, col_name in enumerate(all_columns):
            if col_name.startswith("SEPARATOR_"):
                continue
            elif col_name.startswith("Total_"):
                safe_write(worksheet, 0, col_num, col_name.replace("_", " "), total_header_format)
            elif "_" in col_name and col_name.split("_")[0] in unique_dates:
                safe_write(worksheet, 0, col_num, col_name.replace("_", " "), date_header_format)
            else:
                safe_write(worksheet, 0, col_num, col_name, header_format)

        # Hide all date-specific Product Cost Input columns and Total Product Cost Input column
        for col_num, col_name in enumerate(all_columns):
            if "Product Cost Input" in col_name and col_name != "Product Cost (Input)":
                worksheet.set_column(col_num, col_num, None, None, {'hidden': True})

        # SET UP COLUMN GROUPING
        start_col = 9  # After base columns + separator
        total_columns = len(all_columns)
        
        group_level = 1
        while start_col < total_columns:
            if start_col < len(all_columns) and all_columns[start_col].startswith("SEPARATOR_"):
                start_col += 1
                continue
                
            data_cols_found = 0
            end_col = start_col
            while end_col < total_columns and data_cols_found < 6:  # 6 metrics per date
                if not all_columns[end_col].startswith("SEPARATOR_"):
                    data_cols_found += 1
                if data_cols_found < 6:
                    end_col += 1
            
            if end_col < total_columns:
                worksheet.set_column(
                    start_col, 
                    end_col - 1, 
                    12, 
                    None, 
                    {'level': group_level, 'collapsed': True, 'hidden': True}
                )
            
            start_col = end_col + 1
        
        # Set base column widths
        worksheet.set_column(0, 1, 25)  # Product title and variant title
        worksheet.set_column(2, 7, 15)  # Other base columns
        worksheet.set_column(8, 8, 3)   # Separator column

        worksheet.outline_settings(
            symbols_below=True,
            symbols_right=True,
            auto_style=False
        )

        # Grand total row
        grand_total_row_idx = 1
        safe_write(worksheet, grand_total_row_idx, 0, "GRAND TOTAL", grand_total_format)
        safe_write(worksheet, grand_total_row_idx, 1, "ALL PRODUCTS", grand_total_format)
        
        row = grand_total_row_idx + 1
        product_total_rows = []

        # Group by product and restructure data
        for product, product_df in df.groupby("Product title"):
            # Get canonical product name for campaign lookup
            canonical_product = product_df['Canonical Product'].iloc[0] if 'Canonical Product' in product_df.columns else product
            
            product_total_row_idx = row
            product_total_rows.append(product_total_row_idx)

            # Calculate total net items sold for dynamic formatting
            total_net_items_for_product = product_df["Net items sold"].sum()
            
            # Dynamic color assignment based on threshold
            if total_net_items_for_product < dynamic_threshold:
                product_total_format = product_total_format_low
                variant_format = variant_format_low
            else:
                product_total_format = product_total_format_high
                variant_format = variant_format_high

            # Product total row
            safe_write(worksheet, product_total_row_idx, 0, product, product_total_format)
            safe_write(worksheet, product_total_row_idx, 1, "ALL VARIANTS (TOTAL)", product_total_format)
            
            # Add Total Net items sold for product header only
            safe_write(worksheet, product_total_row_idx, 4, total_net_items_for_product, product_total_format)

            # Group variants within product
            variant_rows = []
            row += 1
            
            for (variant_title), variant_group in product_df.groupby("Product variant title"):
                variant_row_idx = row
                variant_rows.append(variant_row_idx)
                
                # Fill base columns for variant
                safe_write(worksheet, variant_row_idx, 0, "", variant_format)
                safe_write(worksheet, variant_row_idx, 1, variant_title, variant_format)
                
                # Calculate averages for base delivery rate and product cost
                delivery_rates = []
                product_costs = []
                
                for _, row_data in variant_group.iterrows():
                    delivery_rate = row_data.get("Delivery Rate", 0) or 0
                    product_cost = row_data.get("Product Cost (Input)", 0) or 0
                    
                    if delivery_rate > 0:
                        delivery_rates.append(delivery_rate)
                    if product_cost > 0:
                        product_costs.append(product_cost)
                
                avg_delivery_rate = sum(delivery_rates) / len(delivery_rates) if delivery_rates else 0
                avg_product_cost = sum(product_costs) / len(product_costs) if product_costs else 0
                
                safe_write(worksheet, variant_row_idx, 2, round(avg_delivery_rate, 2), variant_format)
                safe_write(worksheet, variant_row_idx, 3, round(avg_product_cost, 2), variant_format)
                
                # Leave other base columns empty (will be calculated via formulas)
                safe_write(worksheet, variant_row_idx, 4, "", variant_format)
                safe_write(worksheet, variant_row_idx, 5, "", variant_format)
                safe_write(worksheet, variant_row_idx, 6, "", variant_format)  # C.P.I
                safe_write(worksheet, variant_row_idx, 7, "", variant_format)  # B.E
                
                # Cell references for base columns
                excel_row = variant_row_idx + 1
                base_delivery_rate_ref = f"{xl_col_to_name(2)}{excel_row}"
                base_product_cost_ref = f"{xl_col_to_name(3)}{excel_row}"
                
                # Fill date-specific data
                for date in unique_dates:
                    date_data = variant_group[variant_group['Date'].astype(str) == date]
                    
                    # Get column indices for this date
                    net_items_col_idx = all_columns.index(f"{date}_Net items sold")
                    avg_price_col_idx = all_columns.index(f"{date}_Avg Price")
                    ad_spend_col_idx = all_columns.index(f"{date}_Ad Spend (USD)")
                    delivery_rate_col_idx = all_columns.index(f"{date}_Delivery Rate")
                    product_cost_input_col_idx = all_columns.index(f"{date}_Product Cost Input")
                    score_col_idx = all_columns.index(f"{date}_Score")
                    
                    # Cell references for this date
                    net_items_ref = f"{xl_col_to_name(net_items_col_idx)}{excel_row}"
                    avg_price_ref = f"{xl_col_to_name(avg_price_col_idx)}{excel_row}"
                    ad_spend_ref = f"{xl_col_to_name(ad_spend_col_idx)}{excel_row}"
                    delivery_rate_ref = f"{xl_col_to_name(delivery_rate_col_idx)}{excel_row}"
                    product_cost_input_ref = f"{xl_col_to_name(product_cost_input_col_idx)}{excel_row}"
                    
                    if not date_data.empty:
                        row_data = date_data.iloc[0]
                        
                        # Actual data for this date (ad spend is now correctly distributed)
                        net_items = row_data.get("Net items sold", 0) or 0
                        avg_price = row_data.get("Product variant price", 0) or 0
                        ad_spend = row_data.get("Ad Spend (USD)", 0) or 0  # This is now correctly calculated
                        delivery_rate = row_data.get("Delivery Rate", 0) or 0
                        product_cost_input = row_data.get("Product Cost (Input)", 0) or 0
                        
                        safe_write(worksheet, variant_row_idx, net_items_col_idx, int(net_items), variant_format)
                        safe_write(worksheet, variant_row_idx, avg_price_col_idx, round(avg_price, 2), variant_format)
                        safe_write(worksheet, variant_row_idx, ad_spend_col_idx, round(ad_spend, 2), variant_format)
                        
                        # Use specific data if available, otherwise link to base
                        if delivery_rate > 0:
                            safe_write(worksheet, variant_row_idx, delivery_rate_col_idx, round(delivery_rate, 2), variant_format)
                        else:
                            worksheet.write_formula(
                                variant_row_idx, delivery_rate_col_idx,
                                f"={base_delivery_rate_ref}",
                                variant_format
                            )
                        
                        if product_cost_input > 0:
                            safe_write(worksheet, variant_row_idx, product_cost_input_col_idx, round(product_cost_input, 2), variant_format)
                        else:
                            worksheet.write_formula(
                                variant_row_idx, product_cost_input_col_idx,
                                f"={base_product_cost_ref}",
                                variant_format
                            )
                        
                    else:
                        # No data for this date
                        safe_write(worksheet, variant_row_idx, net_items_col_idx, 0, variant_format)
                        safe_write(worksheet, variant_row_idx, avg_price_col_idx, 0.00, variant_format)
                        safe_write(worksheet, variant_row_idx, ad_spend_col_idx, 0.00, variant_format)
                        
                        worksheet.write_formula(
                            variant_row_idx, delivery_rate_col_idx,
                            f"={base_delivery_rate_ref}",
                            variant_format
                        )
                        worksheet.write_formula(
                            variant_row_idx, product_cost_input_col_idx,
                            f"={base_product_cost_ref}",
                            variant_format
                        )
                    
                    # SCORE FORMULA (correct for variants)
                    rate_term = f"IF(ISNUMBER({delivery_rate_ref}),IF({delivery_rate_ref}>1,{delivery_rate_ref}/100,{delivery_rate_ref}),0)"
                    score_formula = f'''=IF(AND({avg_price_ref}>0,{net_items_ref}>0),
                        (({avg_price_ref}*{net_items_ref}*{rate_term})
                        -({ad_spend_ref}*100)-(77*{net_items_ref})-(65*{net_items_ref})
                        -({product_cost_input_ref}*{net_items_ref}*{rate_term}))
                        /(({avg_price_ref}*{net_items_ref}*{rate_term})*0.1),0)'''
                    
                    worksheet.write_formula(
                        variant_row_idx, score_col_idx,
                        score_formula,
                        variant_format
                    )
                
                # TOTAL COLUMNS CALCULATIONS FOR VARIANT
                for metric in date_metrics:
                    total_col_idx = all_columns.index(f"Total_{metric}")
                    
                    if metric == "Net items sold":
                        # SUM: Add all date-specific net items sold
                        if len(unique_dates) > 1:
                            date_refs = []
                            for date in unique_dates:
                                date_col_idx = all_columns.index(f"{date}_{metric}")
                                date_refs.append(f"{xl_col_to_name(date_col_idx)}{excel_row}")
                            
                            sum_formula = "+".join(date_refs)
                            worksheet.write_formula(
                                variant_row_idx, total_col_idx,
                                f"={sum_formula}",
                                variant_format
                            )
                        else:
                            single_date_col = all_columns.index(f"{unique_dates[0]}_{metric}")
                            worksheet.write_formula(
                                variant_row_idx, total_col_idx,
                                f"={xl_col_to_name(single_date_col)}{excel_row}",
                                variant_format
                            )
                    
                    elif metric in ["Avg Price", "Delivery Rate", "Product Cost Input"]:
                        # WEIGHTED AVERAGE
                        total_net_items_col_idx = all_columns.index("Total_Net items sold")
                        total_net_items_ref = f"{xl_col_to_name(total_net_items_col_idx)}{excel_row}"
                        
                        if len(unique_dates) > 1:
                            metric_terms = []
                            for date in unique_dates:
                                metric_col_idx = all_columns.index(f"{date}_{metric}")
                                net_items_col_idx = all_columns.index(f"{date}_Net items sold")
                                metric_terms.append(f"{xl_col_to_name(metric_col_idx)}{excel_row}*{xl_col_to_name(net_items_col_idx)}{excel_row}")
                            
                            sumproduct_formula = "+".join(metric_terms)
                            worksheet.write_formula(
                                variant_row_idx, total_col_idx,
                                f"=IF({total_net_items_ref}=0,0,({sumproduct_formula})/{total_net_items_ref})",
                                variant_format
                            )
                        else:
                            single_date_col = all_columns.index(f"{unique_dates[0]}_{metric}")
                            worksheet.write_formula(
                                variant_row_idx, total_col_idx,
                                f"={xl_col_to_name(single_date_col)}{excel_row}",
                                variant_format
                            )
                    
                    elif metric == "Score":
                        # TOTAL SCORE FORMULA (correct calculation using aggregated values)
                        total_avg_price_col_idx = all_columns.index("Total_Avg Price")
                        total_net_items_col_idx = all_columns.index("Total_Net items sold")
                        total_ad_spend_col_idx = all_columns.index("Total_Ad Spend (USD)")
                        total_delivery_rate_col_idx = all_columns.index("Total_Delivery Rate")
                        total_product_cost_col_idx = all_columns.index("Total_Product Cost Input")
                        
                        total_avg_price_ref = f"{xl_col_to_name(total_avg_price_col_idx)}{excel_row}"
                        total_net_items_ref = f"{xl_col_to_name(total_net_items_col_idx)}{excel_row}"
                        total_ad_spend_ref = f"{xl_col_to_name(total_ad_spend_col_idx)}{excel_row}"
                        total_delivery_rate_ref = f"{xl_col_to_name(total_delivery_rate_col_idx)}{excel_row}"
                        total_product_cost_ref = f"{xl_col_to_name(total_product_cost_col_idx)}{excel_row}"
                        
                        total_rate_term = f"IF(ISNUMBER({total_delivery_rate_ref}),IF({total_delivery_rate_ref}>1,{total_delivery_rate_ref}/100,{total_delivery_rate_ref}),0)"
                        total_score_formula = f'''=IF(AND({total_avg_price_ref}>0,{total_net_items_ref}>0),
                            (({total_avg_price_ref}*{total_net_items_ref}*{total_rate_term})
                            -({total_ad_spend_ref}*100)-(77*{total_net_items_ref})-(65*{total_net_items_ref})
                            -({total_product_cost_ref}*{total_net_items_ref}*{total_rate_term}))
                            /(({total_avg_price_ref}*{total_net_items_ref}*{total_rate_term})*0.1),0)'''
                        
                        worksheet.write_formula(
                            variant_row_idx, total_col_idx,
                            total_score_formula,
                            variant_format
                        )
                    
                    else:
                        # SUM: Ad Spend (USD)
                        if len(unique_dates) > 1:
                            date_refs = []
                            for date in unique_dates:
                                date_col_idx = all_columns.index(f"{date}_{metric}")
                                date_refs.append(f"{xl_col_to_name(date_col_idx)}{excel_row}")
                            
                            sum_formula = "+".join(date_refs)
                            worksheet.write_formula(
                                variant_row_idx, total_col_idx,
                                f"={sum_formula}",
                                variant_format
                            )
                        else:
                            single_date_col = all_columns.index(f"{unique_dates[0]}_{metric}")
                            worksheet.write_formula(
                                variant_row_idx, total_col_idx,
                                f"={xl_col_to_name(single_date_col)}{excel_row}",
                                variant_format
                            )
                
                # Calculate base columns using total columns
                total_net_items_col_idx = all_columns.index("Total_Net items sold")
                total_ad_spend_col_idx = all_columns.index("Total_Ad Spend (USD)")
                
                worksheet.write_formula(
                    variant_row_idx, 4,
                    f"={xl_col_to_name(total_net_items_col_idx)}{excel_row}",
                    variant_format
                )
                
                worksheet.write_formula(
                    variant_row_idx, 5,
                    f"={xl_col_to_name(total_ad_spend_col_idx)}{excel_row}",
                    variant_format
                )
                
                # Calculate "C.P.I" for base column (Amount Spent / Net Items Sold)
                worksheet.write_formula(
                    variant_row_idx, 6,
                    f"=ROUND(IF(E{excel_row}=0,0,F{excel_row}/E{excel_row}),2)",
                    variant_format
                )
                
                # MODIFIED: B.E (Break Even) - Reference the PRODUCT TOTAL B.E value for all variants
                # This ensures all variants within a product have the same B.E value calculated at product level
                product_total_excel_row = product_total_row_idx + 1
                worksheet.write_formula(
                    variant_row_idx, 7,
                    f"=${xl_col_to_name(7)}${product_total_excel_row}",  # Absolute reference to product total B.E
                    variant_format
                )
                
                row += 1
            
            # Calculate product totals by aggregating variant rows using RANGES
            if variant_rows:
                first_variant_row = min(variant_rows) + 1
                last_variant_row = max(variant_rows) + 1
                
                # PRODUCT TOTAL CALCULATIONS
                for date in unique_dates:
                    for metric in date_metrics:
                        col_idx = all_columns.index(f"{date}_{metric}")
                        
                        if metric in ["Avg Price", "Delivery Rate", "Product Cost Input"]:
                            # Weighted average based on net items sold for this date using RANGES
                            date_net_items_col_idx = all_columns.index(f"{date}_Net items sold")
                            
                            metric_range = f"{xl_col_to_name(col_idx)}{first_variant_row}:{xl_col_to_name(col_idx)}{last_variant_row}"
                            net_items_range = f"{xl_col_to_name(date_net_items_col_idx)}{first_variant_row}:{xl_col_to_name(date_net_items_col_idx)}{last_variant_row}"
                            
                            worksheet.write_formula(
                                product_total_row_idx, col_idx,
                                f"=IF(SUM({net_items_range})=0,0,SUMPRODUCT({metric_range},{net_items_range})/SUM({net_items_range}))",
                                product_total_format
                            )
                        elif metric == "Score":
                            # CORRECT SCORE CALCULATION FOR PRODUCT TOTAL (using aggregated values)
                            avg_price_idx = all_columns.index(f"{date}_Avg Price")
                            net_items_idx = all_columns.index(f"{date}_Net items sold")
                            ad_spend_idx = all_columns.index(f"{date}_Ad Spend (USD)")
                            delivery_rate_idx = all_columns.index(f"{date}_Delivery Rate")
                            product_cost_idx = all_columns.index(f"{date}_Product Cost Input")
                            
                            avg_price_ref = f"{xl_col_to_name(avg_price_idx)}{product_total_row_idx+1}"
                            net_items_ref = f"{xl_col_to_name(net_items_idx)}{product_total_row_idx+1}"
                            ad_spend_ref = f"{xl_col_to_name(ad_spend_idx)}{product_total_row_idx+1}"
                            delivery_rate_ref = f"{xl_col_to_name(delivery_rate_idx)}{product_total_row_idx+1}"
                            product_cost_ref = f"{xl_col_to_name(product_cost_idx)}{product_total_row_idx+1}"
                            
                            rate_term = f"IF(ISNUMBER({delivery_rate_ref}),IF({delivery_rate_ref}>1,{delivery_rate_ref}/100,{delivery_rate_ref}),0)"
                            score_formula = f'''=IF(AND({avg_price_ref}>0,{net_items_ref}>0),
                                (({avg_price_ref}*{net_items_ref}*{rate_term})
                                -({ad_spend_ref}*100)-(77*{net_items_ref})-(65*{net_items_ref})
                                -({product_cost_ref}*{net_items_ref}*{rate_term}))
                                /(({avg_price_ref}*{net_items_ref}*{rate_term})*0.1),0)'''
                            
                            worksheet.write_formula(
                                product_total_row_idx, col_idx,
                                score_formula,
                                product_total_format
                            )
                        else:
                            # Sum for other metrics using ranges
                            col_range = f"{xl_col_to_name(col_idx)}{first_variant_row}:{xl_col_to_name(col_idx)}{last_variant_row}"
                            worksheet.write_formula(
                                product_total_row_idx, col_idx,
                                f"=SUM({col_range})",
                                product_total_format
                            )
                
                # Calculate product totals for Total columns using RANGES
                for metric in date_metrics:
                    col_idx = all_columns.index(f"Total_{metric}")
                    
                    if metric in ["Avg Price", "Delivery Rate", "Product Cost Input"]:
                        # Weighted average based on total net items sold using RANGES
                        total_net_items_col_idx = all_columns.index("Total_Net items sold")
                        
                        metric_range = f"{xl_col_to_name(col_idx)}{first_variant_row}:{xl_col_to_name(col_idx)}{last_variant_row}"
                        net_items_range = f"{xl_col_to_name(total_net_items_col_idx)}{first_variant_row}:{xl_col_to_name(total_net_items_col_idx)}{last_variant_row}"
                        
                        worksheet.write_formula(
                            product_total_row_idx, col_idx,
                            f"=IF(SUM({net_items_range})=0,0,SUMPRODUCT({metric_range},{net_items_range})/SUM({net_items_range}))",
                            product_total_format
                        )
                    elif metric == "Score":
                        # CORRECT SCORE CALCULATION FOR PRODUCT TOTAL (using aggregated total values)
                        total_avg_price_col_idx = all_columns.index("Total_Avg Price")
                        total_net_items_col_idx = all_columns.index("Total_Net items sold")
                        total_ad_spend_col_idx = all_columns.index("Total_Ad Spend (USD)")
                        total_delivery_rate_col_idx = all_columns.index("Total_Delivery Rate")
                        total_product_cost_col_idx = all_columns.index("Total_Product Cost Input")
                        
                        total_avg_price_ref = f"{xl_col_to_name(total_avg_price_col_idx)}{product_total_row_idx+1}"
                        total_net_items_ref = f"{xl_col_to_name(total_net_items_col_idx)}{product_total_row_idx+1}"
                        total_ad_spend_ref = f"{xl_col_to_name(total_ad_spend_col_idx)}{product_total_row_idx+1}"
                        total_delivery_rate_ref = f"{xl_col_to_name(total_delivery_rate_col_idx)}{product_total_row_idx+1}"
                        total_product_cost_ref = f"{xl_col_to_name(total_product_cost_col_idx)}{product_total_row_idx+1}"
                        
                        total_rate_term = f"IF(ISNUMBER({total_delivery_rate_ref}),IF({total_delivery_rate_ref}>1,{total_delivery_rate_ref}/100,{total_delivery_rate_ref}),0)"
                        total_score_formula = f'''=IF(AND({total_avg_price_ref}>0,{total_net_items_ref}>0),
                            (({total_avg_price_ref}*{total_net_items_ref}*{total_rate_term})
                            -({total_ad_spend_ref}*100)-(77*{total_net_items_ref})-(65*{total_net_items_ref})
                            -({total_product_cost_ref}*{total_net_items_ref}*{total_rate_term}))
                            /(({total_avg_price_ref}*{total_net_items_ref}*{total_rate_term})*0.1),0)'''
                        
                        worksheet.write_formula(
                            product_total_row_idx, col_idx,
                            total_score_formula,
                            product_total_format
                        )
                    else:
                        # Sum for other metrics using ranges
                        col_range = f"{xl_col_to_name(col_idx)}{first_variant_row}:{xl_col_to_name(col_idx)}{last_variant_row}"
                        worksheet.write_formula(
                            product_total_row_idx, col_idx,
                            f"=SUM({col_range})",
                            product_total_format
                        )
                
                # Base columns for product totals
                base_delivery_rate_col = 2
                base_product_cost_col = 3
                base_total_net_items_col = 4
                base_amount_spent_col = 5
                base_cost_per_item_col = 6  # C.P.I
                base_break_even_point_col = 7  # B.E
                total_delivery_rate_col_idx = all_columns.index("Total_Delivery Rate")
                total_product_cost_col_idx = all_columns.index("Total_Product Cost Input")
                total_net_items_col_idx = all_columns.index("Total_Net items sold")
                total_ad_spend_col_idx = all_columns.index("Total_Ad Spend (USD)")
                
                worksheet.write_formula(
                    product_total_row_idx, base_delivery_rate_col,
                    f"={xl_col_to_name(total_delivery_rate_col_idx)}{product_total_row_idx+1}",
                    product_total_format
                )
                
                worksheet.write_formula(
                    product_total_row_idx, base_product_cost_col,
                    f"={xl_col_to_name(total_product_cost_col_idx)}{product_total_row_idx+1}",
                    product_total_format
                )
                
                worksheet.write_formula(
                    product_total_row_idx, base_total_net_items_col,
                    f"={xl_col_to_name(total_net_items_col_idx)}{product_total_row_idx+1}",
                    product_total_format
                )
                
                worksheet.write_formula(
                    product_total_row_idx, base_amount_spent_col,
                    f"={xl_col_to_name(total_ad_spend_col_idx)}{product_total_row_idx+1}",
                    product_total_format
                )
                
                # Calculate "C.P.I" and "B.E" for product total
                product_excel_row = product_total_row_idx + 1
                worksheet.write_formula(
                    product_total_row_idx, base_cost_per_item_col,
                    f"=ROUND(IF(E{product_excel_row}=0,0,F{product_excel_row}/E{product_excel_row}),2)",
                    product_total_format
                )
                
                # B.E (Break Even) for product total - CALCULATE ONCE FOR PRODUCT
                total_avg_price_col_idx = all_columns.index("Total_Avg Price")
                total_delivery_rate_col_idx = all_columns.index("Total_Delivery Rate")
                total_product_cost_col_idx = all_columns.index("Total_Product Cost Input")
                
                total_avg_price_ref = f"{xl_col_to_name(total_avg_price_col_idx)}{product_excel_row}"
                total_net_items_ref = f"{xl_col_to_name(total_net_items_col_idx)}{product_excel_row}"
                total_delivery_rate_ref = f"{xl_col_to_name(total_delivery_rate_col_idx)}{product_excel_row}"
                total_product_cost_ref = f"{xl_col_to_name(total_product_cost_col_idx)}{product_excel_row}"
                
                break_even_formula = f'''=IF(AND({total_avg_price_ref}>0,{total_net_items_ref}>0),
                    ((({total_avg_price_ref}*{total_net_items_ref}*IF(ISNUMBER({total_delivery_rate_ref}),IF({total_delivery_rate_ref}>1,{total_delivery_rate_ref}/100,{total_delivery_rate_ref}),0))
                    -(77*{total_net_items_ref})-(65*{total_net_items_ref})
                    -({total_product_cost_ref}*{total_net_items_ref}*IF(ISNUMBER({total_delivery_rate_ref}),IF({total_delivery_rate_ref}>1,{total_delivery_rate_ref}/100,{total_delivery_rate_ref}),0)))/100)/{total_net_items_ref},0)'''
                
                worksheet.write_formula(
                    product_total_row_idx, base_break_even_point_col,
                    break_even_formula,
                    product_total_format
                )

        # Calculate grand totals using INDIVIDUAL PRODUCT TOTAL ROWS ONLY
        if product_total_rows:
            # Base columns for grand total
            base_delivery_rate_col = 2
            base_product_cost_col = 3
            base_total_net_items_col = 4
            base_amount_spent_col = 5
            base_cost_per_item_col = 6  # C.P.I
            base_break_even_point_col = 7  # B.E
            total_delivery_rate_col_idx = all_columns.index("Total_Delivery Rate")
            total_product_cost_col_idx = all_columns.index("Total_Product Cost Input")
            total_net_items_col_idx = all_columns.index("Total_Net items sold")
            total_ad_spend_col_idx = all_columns.index("Total_Ad Spend (USD)")
            
            worksheet.write_formula(
                grand_total_row_idx, base_delivery_rate_col,
                f"={xl_col_to_name(total_delivery_rate_col_idx)}{grand_total_row_idx+1}",
                grand_total_format
            )
            
            worksheet.write_formula(
                grand_total_row_idx, base_product_cost_col,
                f"={xl_col_to_name(total_product_cost_col_idx)}{grand_total_row_idx+1}",
                grand_total_format
            )
            
            worksheet.write_formula(
                grand_total_row_idx, base_total_net_items_col,
                f"={xl_col_to_name(total_net_items_col_idx)}{grand_total_row_idx+1}",
                grand_total_format
            )
            
            worksheet.write_formula(
                grand_total_row_idx, base_amount_spent_col,
                f"={xl_col_to_name(total_ad_spend_col_idx)}{grand_total_row_idx+1}",
                grand_total_format
            )
            
            # Calculate "C.P.I" and "B.E" for grand total
            grand_excel_row = grand_total_row_idx + 1
            worksheet.write_formula(
                grand_total_row_idx, base_cost_per_item_col,
                f"=ROUND(IF(E{grand_excel_row}=0,0,F{grand_excel_row}/E{grand_excel_row}),2)",
                grand_total_format
            )
            
            # B.E (Break Even) for grand total - CALCULATE ONCE FOR GRAND TOTAL
            total_avg_price_col_idx = all_columns.index("Total_Avg Price")
            total_delivery_rate_col_idx = all_columns.index("Total_Delivery Rate")
            total_product_cost_col_idx = all_columns.index("Total_Product Cost Input")
            
            total_avg_price_ref = f"{xl_col_to_name(total_avg_price_col_idx)}{grand_excel_row}"
            total_net_items_ref = f"{xl_col_to_name(total_net_items_col_idx)}{grand_excel_row}"
            total_delivery_rate_ref = f"{xl_col_to_name(total_delivery_rate_col_idx)}{grand_excel_row}"
            total_product_cost_ref = f"{xl_col_to_name(total_product_cost_col_idx)}{grand_excel_row}"
            
            break_even_formula = f'''=IF(AND({total_avg_price_ref}>0,{total_net_items_ref}>0),
                ((({total_avg_price_ref}*{total_net_items_ref}*IF(ISNUMBER({total_delivery_rate_ref}),IF({total_delivery_rate_ref}>1,{total_delivery_rate_ref}/100,{total_delivery_rate_ref}),0))
                -(77*{total_net_items_ref})-(65*{total_net_items_ref})
                -({total_product_cost_ref}*{total_net_items_ref}*IF(ISNUMBER({total_delivery_rate_ref}),IF({total_delivery_rate_ref}>1,{total_delivery_rate_ref}/100,{total_delivery_rate_ref}),0)))/100)/{total_net_items_ref},0)'''
            
            worksheet.write_formula(
                grand_total_row_idx, base_break_even_point_col,
                break_even_formula,
                grand_total_format
            )
            
            # Date-specific and total columns for grand total using INDIVIDUAL PRODUCT ROWS
            for date in unique_dates:
                for metric in date_metrics:
                    col_idx = all_columns.index(f"{date}_{metric}")
                    
                    if metric in ["Avg Price", "Delivery Rate", "Product Cost Input"]:
                        # Weighted average using individual product total rows
                        date_net_items_col_idx = all_columns.index(f"{date}_Net items sold")
                        
                        metric_refs = []
                        net_items_refs = []
                        for product_row_idx in product_total_rows:
                            product_excel_row = product_row_idx + 1
                            metric_refs.append(f"{xl_col_to_name(col_idx)}{product_excel_row}")
                            net_items_refs.append(f"{xl_col_to_name(date_net_items_col_idx)}{product_excel_row}")
                        
                        sumproduct_terms = []
                        for i in range(len(metric_refs)):
                            sumproduct_terms.append(f"{metric_refs[i]}*{net_items_refs[i]}")
                        
                        sumproduct_formula = "+".join(sumproduct_terms)
                        sum_net_items_formula = "+".join(net_items_refs)
                        
                        worksheet.write_formula(
                            grand_total_row_idx, col_idx,
                            f"=IF(({sum_net_items_formula})=0,0,({sumproduct_formula})/({sum_net_items_formula}))",
                            grand_total_format
                        )
                    elif metric == "Score":
                        # CORRECT SCORE CALCULATION FOR GRAND TOTAL (using aggregated values)
                        avg_price_idx = all_columns.index(f"{date}_Avg Price")
                        net_items_idx = all_columns.index(f"{date}_Net items sold")
                        ad_spend_idx = all_columns.index(f"{date}_Ad Spend (USD)")
                        delivery_rate_idx = all_columns.index(f"{date}_Delivery Rate")
                        product_cost_idx = all_columns.index(f"{date}_Product Cost Input")
                        
                        avg_price_ref = f"{xl_col_to_name(avg_price_idx)}{grand_total_row_idx+1}"
                        net_items_ref = f"{xl_col_to_name(net_items_idx)}{grand_total_row_idx+1}"
                        ad_spend_ref = f"{xl_col_to_name(ad_spend_idx)}{grand_total_row_idx+1}"
                        delivery_rate_ref = f"{xl_col_to_name(delivery_rate_idx)}{grand_total_row_idx+1}"
                        product_cost_ref = f"{xl_col_to_name(product_cost_idx)}{grand_total_row_idx+1}"
                        
                        rate_term = f"IF(ISNUMBER({delivery_rate_ref}),IF({delivery_rate_ref}>1,{delivery_rate_ref}/100,{delivery_rate_ref}),0)"
                        score_formula = f'''=IF(AND({avg_price_ref}>0,{net_items_ref}>0),
                            (({avg_price_ref}*{net_items_ref}*{rate_term})
                            -({ad_spend_ref}*100)-(77*{net_items_ref})-(65*{net_items_ref})
                            -({product_cost_ref}*{net_items_ref}*{rate_term}))
                            /(({avg_price_ref}*{net_items_ref}*{rate_term})*0.1),0)'''
                        
                        worksheet.write_formula(
                            grand_total_row_idx, col_idx,
                            score_formula,
                            grand_total_format
                        )
                    else:
                        # Sum using individual product total rows only
                        sum_refs = []
                        for product_row_idx in product_total_rows:
                            product_excel_row = product_row_idx + 1
                            sum_refs.append(f"{xl_col_to_name(col_idx)}{product_excel_row}")
                        
                        sum_formula = "+".join(sum_refs)
                        worksheet.write_formula(
                            grand_total_row_idx, col_idx,
                            f"={sum_formula}",
                            grand_total_format
                        )
            
            # Total columns for grand total using INDIVIDUAL PRODUCT TOTAL ROWS
            total_net_items_col_idx = all_columns.index("Total_Net items sold")
            
            for metric in date_metrics:
                col_idx = all_columns.index(f"Total_{metric}")
                
                if metric in ["Avg Price", "Delivery Rate", "Product Cost Input"]:
                    # Weighted average using individual product total rows
                    metric_refs = []
                    net_items_refs = []
                    for product_row_idx in product_total_rows:
                        product_excel_row = product_row_idx + 1
                        metric_refs.append(f"{xl_col_to_name(col_idx)}{product_excel_row}")
                        net_items_refs.append(f"{xl_col_to_name(total_net_items_col_idx)}{product_excel_row}")
                    
                    sumproduct_terms = []
                    for i in range(len(metric_refs)):
                        sumproduct_terms.append(f"{metric_refs[i]}*{net_items_refs[i]}")
                    
                    sumproduct_formula = "+".join(sumproduct_terms)
                    sum_net_items_formula = "+".join(net_items_refs)
                    
                    worksheet.write_formula(
                        grand_total_row_idx, col_idx,
                        f"=IF(({sum_net_items_formula})=0,0,({sumproduct_formula})/({sum_net_items_formula}))",
                        grand_total_format
                    )
                elif metric == "Score":
                    # CORRECT SCORE CALCULATION FOR GRAND TOTAL (using aggregated total values)
                    total_avg_price_col_idx = all_columns.index("Total_Avg Price")
                    total_net_items_col_idx = all_columns.index("Total_Net items sold")
                    total_ad_spend_col_idx = all_columns.index("Total_Ad Spend (USD)")
                    total_delivery_rate_col_idx = all_columns.index("Total_Delivery Rate")
                    total_product_cost_col_idx = all_columns.index("Total_Product Cost Input")
                    
                    total_avg_price_ref = f"{xl_col_to_name(total_avg_price_col_idx)}{grand_total_row_idx+1}"
                    total_net_items_ref = f"{xl_col_to_name(total_net_items_col_idx)}{grand_total_row_idx+1}"
                    total_ad_spend_ref = f"{xl_col_to_name(total_ad_spend_col_idx)}{grand_total_row_idx+1}"
                    total_delivery_rate_ref = f"{xl_col_to_name(total_delivery_rate_col_idx)}{grand_total_row_idx+1}"
                    total_product_cost_ref = f"{xl_col_to_name(total_product_cost_col_idx)}{grand_total_row_idx+1}"
                    
                    total_rate_term = f"IF(ISNUMBER({total_delivery_rate_ref}),IF({total_delivery_rate_ref}>1,{total_delivery_rate_ref}/100,{total_delivery_rate_ref}),0)"
                    total_score_formula = f'''=IF(AND({total_avg_price_ref}>0,{total_net_items_ref}>0),
                        (({total_avg_price_ref}*{total_net_items_ref}*{total_rate_term})
                        -({total_ad_spend_ref}*100)-(77*{total_net_items_ref})-(65*{total_net_items_ref})
                        -({total_product_cost_ref}*{total_net_items_ref}*{total_rate_term}))
                        /(({total_avg_price_ref}*{total_net_items_ref}*{total_rate_term})*0.1),0)'''
                    
                    worksheet.write_formula(
                        grand_total_row_idx, col_idx,
                        total_score_formula,
                        grand_total_format
                    )
                else:
                    # Sum using individual product total rows only
                    sum_refs = []
                    for product_row_idx in product_total_rows:
                        product_excel_row = product_row_idx + 1
                        sum_refs.append(f"{xl_col_to_name(col_idx)}{product_excel_row}")
                    
                    sum_formula = "+".join(sum_refs)
                    worksheet.write_formula(
                        grand_total_row_idx, col_idx,
                        f"={sum_formula}",
                        grand_total_format
                    )

        # Freeze panes to keep base columns visible when scrolling
        worksheet.freeze_panes(2, len(base_columns))
    
    return output.getvalue()




def convert_final_campaign_to_excel_staff_with_date_columns_fixed(df, shopify_df=None, selected_days=None):
    """Convert Campaign data to Excel for staff with day-wise lookups and scoring focus"""
    if df.empty:
        return None
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        workbook = writer.book
        
        # ==== MAIN SHEET: Campaign Staff Data ====
        worksheet = workbook.add_worksheet("Campaign Staff")
        writer.sheets["Campaign Staff"] = worksheet

        # Staff-specific formats (with two decimal places)
        header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#BDD7EE", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        date_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#B4C6E7", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        total_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#FFD966", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        grand_total_format = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#EEEE0E", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        product_total_format = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#E7EE94", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        campaign_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        # New remark format with different background color
        remark_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#FFC0CB", "font_name": "Calibri", "font_size": 11  # Light pink background
        })
        remark_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#FFC0CB", "font_name": "Calibri", "font_size": 11  # Light pink background
        })
        # NEW: Exclusion table formats
        exclusion_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#FF9999", "font_name": "Calibri", "font_size": 11
        })
        exclusion_data_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#FFE6E6", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })

        # Check if we have dates
        has_dates = 'Date' in df.columns
        if not has_dates:
            # Fall back to original structure if no dates
            return None  # You would call your original function here
        
        # Get unique dates and sort them
        unique_dates = sorted([str(d) for d in df['Date'].unique() if pd.notna(d) and str(d).strip() != ''])
        if selected_days is None:
            if len(unique_dates) > 0:
                n_days = len(unique_dates)
                selected_days = n_days // 2 if n_days % 2 == 0 else (n_days + 1) // 2
            else:
                selected_days = 1
        
        # Define base columns for staff (CHANGED: "Cost Per Purchase" to "C.P.P" and "Break Even Point" to "B.E")
        base_columns = ["Product Name", "Campaign Name", "Total Amount Spent (USD)", "Purchases", "C.P.P", "B.E"]
        
        # Define staff metrics (simplified - only 6 metrics per date for campaigns)
        # CHANGED: "Cost Per Purchase (USD)" to "C.P.P (USD)"
        date_metrics = ["Delivery status","Amount Spent (USD)", "Purchases", "C.P.P (USD)", "Avg Price", "Delivery Rate", "Score"]
        
        # Build column structure WITH SEPARATOR COLUMNS
        all_columns = base_columns.copy()
        all_columns.append("SEPARATOR_AFTER_BASE")
        
        # Add date-specific columns with separators
        for date in unique_dates:
            for metric in date_metrics:
                all_columns.append(f"{date}_{metric}")
            all_columns.append(f"SEPARATOR_AFTER_{date}")
        
        # Add total columns
        for metric in date_metrics:
            all_columns.append(f"Total_{metric}")
        
        # Add Remark column at the end
        all_columns.append("Remark")

        # FIRST: Identify matched and unmatched campaigns BEFORE processing main sheet
        matched_campaigns = []
        unmatched_campaigns = []
        
        # Check each campaign for Shopify data availability
        for product, product_df in df.groupby("Product"):
            # Check if this product has Shopify data (day-wise lookups)
            has_shopify_data = (product in product_date_avg_prices and 
                              any(date in product_date_avg_prices[product] for date in unique_dates) or
                              product in product_date_delivery_rates and 
                              any(date in product_date_delivery_rates[product] for date in unique_dates) or
                              product in product_date_cost_inputs and 
                              any(date in product_date_cost_inputs[product] for date in unique_dates))
            
            for campaign_name, campaign_group in product_df.groupby("Campaign Name"):
                total_amount_spent_usd = campaign_group.get("Amount Spent (USD)", 0).sum() if "Amount Spent (USD)" in campaign_group.columns else 0
                total_purchases = campaign_group.get("Purchases", 0).sum() if "Purchases" in campaign_group.columns else 0
                
                # Calculate cost per purchase for sorting
                cost_per_purchase = 0
                if total_purchases > 0:
                    cost_per_purchase = total_amount_spent_usd / total_purchases
                elif total_amount_spent_usd > 0 and total_purchases == 0:
                    cost_per_purchase = total_amount_spent_usd / 1  # Use 1 for zero purchases but has spending
                
                campaign_info = {
                    'Product': str(product) if pd.notna(product) else '',
                    'Campaign Name': str(campaign_name) if pd.notna(campaign_name) else '',
                    'Amount Spent (USD)': round(float(total_amount_spent_usd), 2) if pd.notna(total_amount_spent_usd) else 0.0,
                    'Purchases': int(total_purchases) if pd.notna(total_purchases) else 0,
                    'Cost Per Purchase': cost_per_purchase,
                    'Has Shopify Data': has_shopify_data,
                    'Dates': sorted([str(d) for d in campaign_group['Date'].unique() if pd.notna(d)]),
                    'Campaign Group': campaign_group  # Store the data group for processing
                }
                
                if has_shopify_data:
                    matched_campaigns.append(campaign_info)
                else:
                    unmatched_campaigns.append(campaign_info)
        
        # FILTER: Create a filtered DataFrame that ONLY contains matched campaigns
        unmatched_campaign_keys = set()
        for campaign in unmatched_campaigns:
            unmatched_campaign_keys.add((campaign['Product'], campaign['Campaign Name']))
        
        # Filter the main DataFrame to exclude unmatched campaigns
        filtered_df_rows = []
        for _, row in df.iterrows():
            campaign_key = (str(row['Product']) if pd.notna(row['Product']) else '', 
                           str(row['Campaign Name']) if pd.notna(row['Campaign Name']) else '')
            if campaign_key not in unmatched_campaign_keys:
                filtered_df_rows.append(row)
        
        # Create filtered DataFrame with only matched campaigns
        if filtered_df_rows:
            filtered_df = pd.DataFrame(filtered_df_rows)
        else:
            # If no matched campaigns, create empty DataFrame with same structure
            filtered_df = df.iloc[0:0].copy()
        
        # Use filtered_df for all main sheet calculations
        df_main = filtered_df

        # NEW: Check for products with zero product cost input and zero delivery rate
        excluded_products = []
        valid_products = []
        
        for product, product_df in df_main.groupby("Product"):
            # Check if product has zero cost input and zero delivery rate across all dates
            has_valid_cost = False
            has_valid_delivery_rate = False
            
            for date in unique_dates:
                date_cost = product_date_cost_inputs.get(product, {}).get(date, 0)
                date_delivery_rate = product_date_delivery_rates.get(product, {}).get(date, 0)
                
                if date_cost > 0:
                    has_valid_cost = True
                if date_delivery_rate > 0:
                    has_valid_delivery_rate = True
            
            # If both cost input and delivery rate are zero, exclude from main calculations
            if not has_valid_cost and not has_valid_delivery_rate:
                total_amount_spent = product_df["Amount Spent (USD)"].sum()
                total_purchases = product_df["Purchases"].sum()
                campaign_count = len(product_df.groupby("Campaign Name"))
                
                excluded_products.append({
                    'Product': str(product),
                    'Campaign Count': campaign_count,
                    'Total Amount Spent (USD)': round(total_amount_spent, 2),
                    'Total Purchases': int(total_purchases),
                    'Reason': 'Product cost input = 0 and delivery rate = 0'
                })
            else:
                valid_products.append((product, product_df))

        # Write headers (skip separator columns)
        for col_num, col_name in enumerate(all_columns):
            if col_name.startswith("SEPARATOR_"):
                continue
            elif col_name == "Remark":
                safe_write(worksheet, 0, col_num, col_name, remark_header_format)
            elif col_name.startswith("Total_"):
                safe_write(worksheet, 0, col_num, col_name.replace("_", " "), total_header_format)
            elif "_" in col_name and col_name.split("_")[0] in unique_dates:
                safe_write(worksheet, 0, col_num, col_name.replace("_", " "), date_header_format)
            else:
                safe_write(worksheet, 0, col_num, col_name, header_format)

        # SET UP COLUMN GROUPING
        start_col = 7  # After base columns + separator (now 6 base columns + 1 separator)
        total_columns = len(all_columns)
        
        group_level = 1
        while start_col < total_columns:
            if start_col < len(all_columns) and all_columns[start_col].startswith("SEPARATOR_"):
                start_col += 1
                continue
                
            data_cols_found = 0
            end_col = start_col
            while end_col < total_columns and data_cols_found < 7:  # 6 metrics per date
                if not all_columns[end_col].startswith("SEPARATOR_"):
                    data_cols_found += 1
                if data_cols_found < 7:
                    end_col += 1
            
            if end_col < total_columns:
                worksheet.set_column(
                    start_col, 
                    end_col - 1, 
                    12, 
                    None, 
                    {'level': group_level, 'collapsed': True, 'hidden': True}
                )
            
            start_col = end_col + 1
        
        # Set base column widths
        worksheet.set_column(0, 0, 25)  # Product Name
        worksheet.set_column(1, 1, 30)  # Campaign Name
        worksheet.set_column(2, 2, 20)  # Total Amount Spent (USD)
        worksheet.set_column(3, 3, 15)  # Purchases
        worksheet.set_column(4, 4, 18)  # C.P.P
        worksheet.set_column(5, 5, 22)  # B.E
        worksheet.set_column(6, 6, 3)   # Separator column
        
        # Set width for Remark column (find its index and set width)
        remark_col_idx = all_columns.index("Remark")
        worksheet.set_column(remark_col_idx, remark_col_idx, 35)  # Remark column

        # Configure outline settings
        worksheet.outline_settings(
            symbols_below=True,
            symbols_right=True,
            auto_style=False
        )

        # Grand total row
        grand_total_row_idx = 1
        safe_write(worksheet, grand_total_row_idx, 0, "ALL VALID PRODUCTS", grand_total_format)
        safe_write(worksheet, grand_total_row_idx, 1, "GRAND TOTAL", grand_total_format)
        
        # Add empty remark for grand total
        remark_col_idx = all_columns.index("Remark")
        safe_write(worksheet, grand_total_row_idx, remark_col_idx, "", remark_format)

        row = grand_total_row_idx + 1
        product_total_rows = []

        # NEW: Pre-calculate product-level delivery rates AND average prices for Total columns (ONLY FOR VALID PRODUCTS)
        product_total_delivery_rates = {}
        product_total_avg_prices = {}
        
        # FIXED: Pre-calculate product-level weighted average cost inputs for grand total calculation
        product_total_cost_inputs = {}
        
        for product, product_df in valid_products:
            # Calculate weighted average delivery rate for this product across all dates
            total_purchases_delivery = 0
            weighted_delivery_rate_sum = 0
            
            # Calculate weighted average price for this product across all dates
            total_purchases_price = 0
            weighted_avg_price_sum = 0
            
            # FIXED: Calculate weighted average cost input for this product across all dates
            total_purchases_cost = 0
            weighted_cost_input_sum = 0
            
            for date in unique_dates:
                date_delivery_rate = product_date_delivery_rates.get(product, {}).get(date, 0)
                date_avg_price = product_date_avg_prices.get(product, {}).get(date, 0)
                date_cost_input = product_date_cost_inputs.get(product, {}).get(date, 0)
                date_purchases = product_df[product_df['Date'].astype(str) == date]['Purchases'].sum() if 'Purchases' in product_df.columns else 0
                
                # For delivery rate calculation
                total_purchases_delivery += date_purchases
                weighted_delivery_rate_sum += date_delivery_rate * date_purchases
                
                # For average price calculation
                total_purchases_price += date_purchases
                weighted_avg_price_sum += date_avg_price * date_purchases
                
                # FIXED: For cost input calculation
                total_purchases_cost += date_purchases
                weighted_cost_input_sum += date_cost_input * date_purchases
            
            # Calculate weighted average delivery rate for this product
            if total_purchases_delivery > 0:
                product_total_delivery_rates[product] = weighted_delivery_rate_sum / total_purchases_delivery
            else:
                product_total_delivery_rates[product] = 0
            
            # Calculate weighted average price for this product
            if total_purchases_price > 0:
                product_total_avg_prices[product] = weighted_avg_price_sum / total_purchases_price
            else:
                product_total_avg_prices[product] = 0
            
            # FIXED: Calculate weighted average cost input for this product
            if total_purchases_cost > 0:
                product_total_cost_inputs[product] = weighted_cost_input_sum / total_purchases_cost
            else:
                product_total_cost_inputs[product] = 0

        # NEW: Calculate total purchases per product for sorting (ONLY FOR VALID PRODUCTS)
        product_purchase_totals = []
        for product, product_df in valid_products:
            total_purchases_for_product = product_df["Purchases"].sum() if "Purchases" in product_df.columns else 0
            product_purchase_totals.append((product, total_purchases_for_product, product_df))
        
        # NEW: Sort products by total purchases in descending order
        product_purchase_totals.sort(key=lambda x: x[1], reverse=True)

        # FIXED: Create mapping of product to its row index for correct reference in grand total calculations
        product_to_row_mapping = {}

        # Group by product and restructure data (NOW USING SORTED ORDER BY PURCHASES AND ONLY VALID PRODUCTS)
        for product, total_purchases_for_product, product_df in product_purchase_totals:
            product_total_row_idx = row
            product_total_rows.append(product_total_row_idx)
            
            # FIXED: Store mapping of product to its row index
            product_to_row_mapping[product] = product_total_row_idx

            # Calculate totals for this product
            total_amount_spent_for_product = product_df["Amount Spent (USD)"].sum()

            # Product total row
            safe_write(worksheet, product_total_row_idx, 0, product, product_total_format)
            safe_write(worksheet, product_total_row_idx, 1, "ALL CAMPAIGNS (TOTAL)", product_total_format)
            
            # Add totals for product header only
            safe_write(worksheet, product_total_row_idx, 2, round(total_amount_spent_for_product, 2), product_total_format)
            safe_write(worksheet, product_total_row_idx, 3, total_purchases_for_product, product_total_format)
            
            # Add remark for products with zero total amount spent
            remark_col_idx = all_columns.index("Remark")
            if total_amount_spent_for_product == 0:
                safe_write(worksheet, product_total_row_idx, remark_col_idx, "Issue with this campaign data - zero spending", remark_format)
            else:
                safe_write(worksheet, product_total_row_idx, remark_col_idx, "", remark_format)

            # NEW: Pre-calculate cost per purchase for each campaign to enable sorting (using filtered data)
            campaign_cpp_list = []
            for campaign_name, campaign_group in product_df.groupby("Campaign Name"):
                total_amount_spent_usd = campaign_group.get("Amount Spent (USD)", 0).sum() if "Amount Spent (USD)" in campaign_group.columns else 0
                total_purchases = campaign_group.get("Purchases", 0).sum() if "Purchases" in campaign_group.columns else 0
                
                # Calculate cost per purchase for sorting
                cost_per_purchase = 0
                if total_purchases > 0:
                    cost_per_purchase = total_amount_spent_usd / total_purchases
                elif total_amount_spent_usd > 0 and total_purchases == 0:
                    cost_per_purchase = total_amount_spent_usd / 1  # Use 1 for zero purchases but has spending
                
                campaign_info = {
                    'Campaign Name': campaign_name,
                    'Cost Per Purchase': cost_per_purchase,
                    'Campaign Group': campaign_group  # Store the data group for processing
                }
                
                campaign_cpp_list.append(campaign_info)
            
            # NEW: Sort campaigns by cost per purchase in ascending order
            campaign_cpp_list.sort(key=lambda x: x['Cost Per Purchase'])

            # Group campaigns within product (now sorted by cost per purchase)
            campaign_rows = []
            row += 1
            
            for campaign_info in campaign_cpp_list:
                campaign_name = campaign_info['Campaign Name']
                campaign_group = campaign_info['Campaign Group']
                
                campaign_row_idx = row
                campaign_rows.append(campaign_row_idx)
                
                # Fill base columns for campaign
                safe_write(worksheet, campaign_row_idx, 0, product, campaign_format)
                safe_write(worksheet, campaign_row_idx, 1, campaign_name, campaign_format)
                
                # Leave base columns empty for campaigns (will be calculated via formulas)
                safe_write(worksheet, campaign_row_idx, 2, "", campaign_format)
                safe_write(worksheet, campaign_row_idx, 3, "", campaign_format)
                safe_write(worksheet, campaign_row_idx, 4, "", campaign_format)
                safe_write(worksheet, campaign_row_idx, 5, "", campaign_format)
                
                # Add remark for campaigns (empty for individual campaigns)
                remark_col_idx = all_columns.index("Remark")
                safe_write(worksheet, campaign_row_idx, remark_col_idx, "", remark_format)
                
                # Cell references for Excel formulas
                excel_row = campaign_row_idx + 1
                
                # Fill date-specific data and formulas
                for date in unique_dates:
                    date_data = campaign_group[campaign_group['Date'].astype(str) == date]
                    
                    # Get column indices for this date (UPDATED: using "C.P.P (USD)")
                    delivery_status_col_idx = all_columns.index(f"{date}_Delivery status")
                    amount_spent_col_idx = all_columns.index(f"{date}_Amount Spent (USD)")
                    purchases_col_idx = all_columns.index(f"{date}_Purchases")
                    cost_per_purchase_col_idx = all_columns.index(f"{date}_C.P.P (USD)")
                    avg_price_col_idx = all_columns.index(f"{date}_Avg Price")
                    delivery_rate_col_idx = all_columns.index(f"{date}_Delivery Rate")
                    score_col_idx = all_columns.index(f"{date}_Score")
                    
                    # Cell references for this date
                    amount_spent_ref = f"{xl_col_to_name(amount_spent_col_idx)}{excel_row}"
                    purchases_ref = f"{xl_col_to_name(purchases_col_idx)}{excel_row}"
                    avg_price_ref = f"{xl_col_to_name(avg_price_col_idx)}{excel_row}"
                    delivery_rate_ref = f"{xl_col_to_name(delivery_rate_col_idx)}{excel_row}"
                    
                    # VALUES FROM DAY-WISE LOOKUPS - Apply to ALL campaigns of this product for this date
                    
                    # FIXED: Average Price - Reference the PRODUCT TOTAL Avg Price for this date
                    # This ensures all campaigns within a product have the same Avg Price calculated at product level
                    product_total_excel_row = product_total_row_idx + 1
                    product_avg_price_col_idx = all_columns.index(f"{date}_Avg Price")
                    worksheet.write_formula(
                        campaign_row_idx, avg_price_col_idx,
                        f"=${xl_col_to_name(product_avg_price_col_idx)}${product_total_excel_row}",  # Absolute reference to product total Avg Price
                        campaign_format
                    )
                    
                    # FIXED: Delivery Rate - Reference the PRODUCT TOTAL Delivery Rate for this date
                    # This ensures all campaigns within a product have the same Delivery Rate calculated at product level
                    product_delivery_rate_col_idx = all_columns.index(f"{date}_Delivery Rate")
                    worksheet.write_formula(
                        campaign_row_idx, delivery_rate_col_idx,
                        f"=${xl_col_to_name(product_delivery_rate_col_idx)}${product_total_excel_row}",  # Absolute reference to product total Delivery Rate
                        campaign_format
                    )
                    
                    if not date_data.empty:
                        row_data = date_data.iloc[0]
                        
                        # Delivery status - from campaign data
                        delivery_status_raw = row_data.get("Delivery status", "")
                        if pd.notna(delivery_status_raw) and str(delivery_status_raw).strip() != "":
                            delivery_status_normalized = str(delivery_status_raw).strip().lower()
                            # Consider "recently completed" and "inactive" as the same (Inactive)
                            if "active" in delivery_status_normalized and "inactive" not in delivery_status_normalized:
                                delivery_status = "Active"
                            else:
                                delivery_status = "Inactive"
                        else:
                            delivery_status = ""
                        safe_write(worksheet, campaign_row_idx, delivery_status_col_idx, delivery_status, campaign_format)
                        
                        # Amount Spent (USD) - from campaign data
                        amount_spent = row_data.get("Amount Spent (USD)", 0) or 0
                        safe_write(worksheet, campaign_row_idx, amount_spent_col_idx, round(float(amount_spent), 2), campaign_format)
                        
                        # Purchases - from campaign data  
                        purchases = row_data.get("Purchases", 0) or 0
                        safe_write(worksheet, campaign_row_idx, purchases_col_idx, purchases, campaign_format)
                        
                    else:
                        # No data for this date
                        safe_write(worksheet, campaign_row_idx, delivery_status_col_idx, "", campaign_format)
                        safe_write(worksheet, campaign_row_idx, amount_spent_col_idx, 0, campaign_format)
                        safe_write(worksheet, campaign_row_idx, purchases_col_idx, 0, campaign_format)
                    
                    # FORMULAS for calculated fields
                    
                    # C.P.P (USD) = Amount Spent (USD) / Purchases (FIXED for zero purchases)
                    worksheet.write_formula(
                        campaign_row_idx, cost_per_purchase_col_idx,
                        f"=IF(AND({purchases_ref}=0,{amount_spent_ref}=0),0,{amount_spent_ref}/IF(AND({purchases_ref}=0,{amount_spent_ref}>0),1,{purchases_ref}))",
                        campaign_format
                    )
                    
                    # SCORE FORMULA for staff (FIXED for zero purchases)
                    rate_term = f"IF(ISNUMBER({delivery_rate_ref}),IF({delivery_rate_ref}>1,{delivery_rate_ref}/100,{delivery_rate_ref}),0)"
                    
                    # Get product cost from day-wise lookup for this product and date  
                    date_product_cost = product_date_cost_inputs.get(product, {}).get(date, 0)
                    
                    # Modified score formula to handle zero purchases case
                    score_formula = f"""=IF(AND({avg_price_ref}>0,OR({purchases_ref}>0,AND({purchases_ref}=0,{amount_spent_ref}>0))),
                        (({avg_price_ref}*{purchases_ref}*{rate_term})
                        -({amount_spent_ref}*100)-(77*{purchases_ref})-(65*{purchases_ref})
                        -({date_product_cost}*{purchases_ref}*{rate_term}))
                         /(({avg_price_ref}*IF(AND({purchases_ref}=0,{amount_spent_ref}>0),1,{purchases_ref})*{rate_term})*0.1),0)"""
                    
                    worksheet.write_formula(
                        campaign_row_idx, score_col_idx,
                        score_formula,
                        campaign_format
                    )
                
                # TOTAL COLUMNS CALCULATIONS FOR CAMPAIGN
                for metric in date_metrics:
                    total_col_idx = all_columns.index(f"Total_{metric}")
                    if metric == "Delivery status":
                        # For Delivery status, just leave it empty in totals (not applicable)
                        safe_write(worksheet, campaign_row_idx, total_col_idx, "", campaign_format)
                    
                    elif metric in ["Avg Price", "Delivery Rate"]:
                        # FIXED: Reference the PRODUCT TOTAL for these metrics in Total columns too
                        # This ensures all campaigns within a product have the same Total Avg Price and Total Delivery Rate
                        product_total_excel_row = product_total_row_idx + 1
                        product_total_metric_col_idx = all_columns.index(f"Total_{metric}")
                        worksheet.write_formula(
                            campaign_row_idx, total_col_idx,
                            f"=${xl_col_to_name(product_total_metric_col_idx)}${product_total_excel_row}",  # Absolute reference to product total
                            campaign_format
                        )
                    
                    elif metric == "C.P.P (USD)":  # FIXED for zero purchases
                        # CALCULATED: Total Amount Spent / Total Purchases
                        total_amount_spent_col_idx = all_columns.index("Total_Amount Spent (USD)")
                        total_purchases_col_idx = all_columns.index("Total_Purchases")
                        total_amount_spent_ref = f"{xl_col_to_name(total_amount_spent_col_idx)}{excel_row}"
                        total_purchases_ref = f"{xl_col_to_name(total_purchases_col_idx)}{excel_row}"
                        
                        worksheet.write_formula(
                            campaign_row_idx, total_col_idx,
                            f"=IF(AND({total_purchases_ref}=0,{total_amount_spent_ref}=0),0,{total_amount_spent_ref}/IF(AND({total_purchases_ref}=0,{total_amount_spent_ref}>0),1,{total_purchases_ref}))",
                            campaign_format
                        )
                    
                    elif metric == "Score":
                        # TOTAL SCORE FORMULA (FIXED for zero purchases)
                        total_avg_price_col_idx = all_columns.index("Total_Avg Price")
                        total_purchases_col_idx = all_columns.index("Total_Purchases")
                        total_amount_spent_col_idx = all_columns.index("Total_Amount Spent (USD)")
                        total_delivery_rate_col_idx = all_columns.index("Total_Delivery Rate")
                        
                        total_avg_price_ref = f"{xl_col_to_name(total_avg_price_col_idx)}{excel_row}"
                        total_purchases_ref = f"{xl_col_to_name(total_purchases_col_idx)}{excel_row}"
                        total_amount_spent_ref = f"{xl_col_to_name(total_amount_spent_col_idx)}{excel_row}"
                        total_delivery_rate_ref = f"{xl_col_to_name(total_delivery_rate_col_idx)}{excel_row}"
                        
                        total_rate_term = f"IF(ISNUMBER({total_delivery_rate_ref}),IF({total_delivery_rate_ref}>1,{total_delivery_rate_ref}/100,{total_delivery_rate_ref}),0)"
                        
                        # FIXED: Use the pre-calculated product-level weighted average cost input
                        avg_product_cost = product_total_cost_inputs.get(product, 0)
                        
                        # Modified total score formula to handle zero purchases case
                        total_score_formula = f"""=IF(AND({total_avg_price_ref}>0,OR({total_purchases_ref}>0,AND({total_purchases_ref}=0,{total_amount_spent_ref}>0))),
                              (({total_avg_price_ref}*{total_purchases_ref}*{total_rate_term})
                              -({total_amount_spent_ref}*100)-(77*{total_purchases_ref})-(65*{total_purchases_ref})
                               -({avg_product_cost}*{total_purchases_ref}*{total_rate_term}))
                               /(({total_avg_price_ref}*IF(AND({total_purchases_ref}=0,{total_amount_spent_ref}>0),1,{total_purchases_ref})*{total_rate_term})*0.1),0)"""
                        
                        worksheet.write_formula(
                            campaign_row_idx, total_col_idx,
                            total_score_formula,
                            campaign_format
                        )
                    
                    else:
                        # SUM: Amount Spent (USD) and Purchases
                        if len(unique_dates) > 1:
                            date_refs = []
                            for date in unique_dates:
                                date_col_idx = all_columns.index(f"{date}_{metric}")
                                date_refs.append(f"{xl_col_to_name(date_col_idx)}{excel_row}")
                            
                            sum_formula = "+".join(date_refs)
                            worksheet.write_formula(
                                campaign_row_idx, total_col_idx,
                                f"={sum_formula}",
                                campaign_format
                            )
                        else:
                            single_date_col = all_columns.index(f"{unique_dates[0]}_{metric}")
                            worksheet.write_formula(
                                campaign_row_idx, total_col_idx,
                                f"={xl_col_to_name(single_date_col)}{excel_row}",
                                campaign_format
                            )
                
                # Calculate base columns for campaign (link to total columns)
                total_amount_spent_col_idx = all_columns.index("Total_Amount Spent (USD)")
                total_purchases_col_idx = all_columns.index("Total_Purchases")
                total_cost_per_purchase_col_idx = all_columns.index("Total_C.P.P (USD)")
                
                worksheet.write_formula(
                    campaign_row_idx, 2,
                    f"={xl_col_to_name(total_amount_spent_col_idx)}{excel_row}",
                    campaign_format
                )
                
                worksheet.write_formula(
                    campaign_row_idx, 3,
                    f"={xl_col_to_name(total_purchases_col_idx)}{excel_row}",
                    campaign_format
                )
                
                worksheet.write_formula(
                    campaign_row_idx, 4,
                    f"={xl_col_to_name(total_cost_per_purchase_col_idx)}{excel_row}",
                    campaign_format
                )
                
                # MODIFIED: B.E (Break Even) - Reference the PRODUCT TOTAL B.E value for all campaigns
                # This ensures all campaigns within a product have the same B.E value calculated at product level
                product_total_excel_row = product_total_row_idx + 1
                worksheet.write_formula(
                    campaign_row_idx, 5,
                    f"=${xl_col_to_name(5)}${product_total_excel_row}",  # Absolute reference to product total B.E
                    campaign_format
                )
                
                row += 1
            
            # Calculate product totals by aggregating campaign rows using RANGES
            if campaign_rows:
                first_campaign_row = min(campaign_rows) + 1
                last_campaign_row = max(campaign_rows) + 1
                
                # PRODUCT TOTAL CALCULATIONS
                for date in unique_dates:
                    for metric in date_metrics:
                        col_idx = all_columns.index(f"{date}_{metric}")
                        
                        if metric == "Delivery status":
                            # For Delivery status in product totals, leave empty (not applicable)
                            safe_write(worksheet, product_total_row_idx, col_idx, "", product_total_format)
                        
                        elif metric in ["Avg Price", "Delivery Rate"]:
                            # Use day-wise lookup data directly for product total (single value for all campaigns)
                            if metric == "Avg Price":
                                date_avg_price = product_date_avg_prices.get(product, {}).get(date, 0)
                                safe_write(worksheet, product_total_row_idx, col_idx, round(float(date_avg_price), 2), product_total_format)
                            else:  # Delivery Rate
                                date_delivery_rate = product_date_delivery_rates.get(product, {}).get(date, 0)
                                safe_write(worksheet, product_total_row_idx, col_idx, round(float(date_delivery_rate), 2), product_total_format)
                        elif metric == "C.P.P (USD)":  # FIXED for zero purchases
                            # Calculate based on totals for this date
                            amount_spent_idx = all_columns.index(f"{date}_Amount Spent (USD)")
                            purchases_idx = all_columns.index(f"{date}_Purchases")
                            worksheet.write_formula(
                                product_total_row_idx, col_idx,
                                f"=IF(AND({xl_col_to_name(purchases_idx)}{product_total_row_idx+1}=0,{xl_col_to_name(amount_spent_idx)}{product_total_row_idx+1}=0),0,{xl_col_to_name(amount_spent_idx)}{product_total_row_idx+1}/IF(AND({xl_col_to_name(purchases_idx)}{product_total_row_idx+1}=0,{xl_col_to_name(amount_spent_idx)}{product_total_row_idx+1}>0),1,{xl_col_to_name(purchases_idx)}{product_total_row_idx+1}))",
                                product_total_format
                            )
                        elif metric == "Score":
                            # FIXED SCORE CALCULATION FOR PRODUCT TOTAL (using aggregated values)
                            avg_price_idx = all_columns.index(f"{date}_Avg Price")
                            purchases_idx = all_columns.index(f"{date}_Purchases")
                            amount_spent_idx = all_columns.index(f"{date}_Amount Spent (USD)")
                            delivery_rate_idx = all_columns.index(f"{date}_Delivery Rate")
                            
                            avg_price_ref = f"{xl_col_to_name(avg_price_idx)}{product_total_row_idx+1}"
                            purchases_ref = f"{xl_col_to_name(purchases_idx)}{product_total_row_idx+1}"
                            amount_spent_ref = f"{xl_col_to_name(amount_spent_idx)}{product_total_row_idx+1}"
                            delivery_rate_ref = f"{xl_col_to_name(delivery_rate_idx)}{product_total_row_idx+1}"
                            
                            rate_term = f"IF(ISNUMBER({delivery_rate_ref}),IF({delivery_rate_ref}>1,{delivery_rate_ref}/100,{delivery_rate_ref}),0)"
                            
                            # Get product cost for this date
                            date_product_cost = product_date_cost_inputs.get(product, {}).get(date, 0)
                            
                            score_formula = f'''=IF(AND({avg_price_ref}>0,OR({purchases_ref}>0,AND({purchases_ref}=0,{amount_spent_ref}>0))),
                                   (({avg_price_ref}*IF(AND({purchases_ref}=0,{amount_spent_ref}>0),1,{purchases_ref})*{rate_term})
                                   -({amount_spent_ref}*100)-(77*IF(AND({purchases_ref}=0,{amount_spent_ref}>0),1,{purchases_ref}))-(65*IF(AND({purchases_ref}=0,{amount_spent_ref}>0),1,{purchases_ref}))
                                   -({date_product_cost}*IF(AND({purchases_ref}=0,{amount_spent_ref}>0),1,{purchases_ref})*{rate_term}))
                                     /(({avg_price_ref}*IF(AND({purchases_ref}=0,{amount_spent_ref}>0),1,{purchases_ref})*{rate_term})*0.1),0)'''
                            
                            worksheet.write_formula(
                                product_total_row_idx, col_idx,
                                score_formula,
                                product_total_format
                            )
                        else:
                            # Sum for other metrics using ranges
                            col_range = f"{xl_col_to_name(col_idx)}{first_campaign_row}:{xl_col_to_name(col_idx)}{last_campaign_row}"
                            worksheet.write_formula(
                                product_total_row_idx, col_idx,
                                f"=SUM({col_range})",
                                product_total_format
                            )
                
                # Calculate product totals for Total columns using RANGES
                for metric in date_metrics:
                    col_idx = all_columns.index(f"Total_{metric}")
                    
                    if metric in ["Avg Price", "Delivery Rate"]:
                        # Use day-wise lookup data directly for product total (single value for all campaigns)
                        if metric == "Avg Price":
                            # Calculate weighted average across all dates for this product
                            total_purchases_col_idx = all_columns.index("Total_Purchases")
                            total_purchases_ref = f"{xl_col_to_name(total_purchases_col_idx)}{product_total_row_idx+1}"
                            
                            if len(unique_dates) > 1:
                                metric_terms = []
                                for date in unique_dates:
                                    date_avg_price = product_date_avg_prices.get(product, {}).get(date, 0)
                                    purchases_col_idx = all_columns.index(f"{date}_Purchases")
                                    purchases_ref = f"{xl_col_to_name(purchases_col_idx)}{product_total_row_idx+1}"
                                    metric_terms.append(f"{date_avg_price}*{purchases_ref}")
                                
                                sumproduct_formula = "+".join(metric_terms)
                                worksheet.write_formula(
                                    product_total_row_idx, col_idx,
                                    f"=IF({total_purchases_ref}=0,0,({sumproduct_formula})/{total_purchases_ref})",
                                    product_total_format
                                )
                            else:
                                date_avg_price = product_date_avg_prices.get(product, {}).get(unique_dates[0], 0)
                                safe_write(worksheet, product_total_row_idx, col_idx, round(float(date_avg_price), 2), product_total_format)
                        else:  # Delivery Rate
                            # Calculate weighted average across all dates for this product
                            total_purchases_col_idx = all_columns.index("Total_Purchases")
                            total_purchases_ref = f"{xl_col_to_name(total_purchases_col_idx)}{product_total_row_idx+1}"
                            
                            if len(unique_dates) > 1:
                                metric_terms = []
                                for date in unique_dates:
                                    date_delivery_rate = product_date_delivery_rates.get(product, {}).get(date, 0)
                                    purchases_col_idx = all_columns.index(f"{date}_Purchases")
                                    purchases_ref = f"{xl_col_to_name(purchases_col_idx)}{product_total_row_idx+1}"
                                    metric_terms.append(f"{date_delivery_rate}*{purchases_ref}")
                                
                                sumproduct_formula = "+".join(metric_terms)
                                worksheet.write_formula(
                                    product_total_row_idx, col_idx,
                                    f"=IF({total_purchases_ref}=0,0,({sumproduct_formula})/{total_purchases_ref})",
                                    product_total_format
                                )
                            else:
                                date_delivery_rate = product_date_delivery_rates.get(product, {}).get(unique_dates[0], 0)
                                safe_write(worksheet, product_total_row_idx, col_idx, round(float(date_delivery_rate), 2), product_total_format)
                    elif metric == "C.P.P (USD)":  # FIXED for zero purchases
                        # Calculate based on totals
                        total_amount_spent_idx = all_columns.index("Total_Amount Spent (USD)")
                        total_purchases_idx = all_columns.index("Total_Purchases")
                        worksheet.write_formula(
                            product_total_row_idx, col_idx,
                            f"=IF(AND({xl_col_to_name(total_purchases_idx)}{product_total_row_idx+1}=0,{xl_col_to_name(total_amount_spent_idx)}{product_total_row_idx+1}=0),0,{xl_col_to_name(total_amount_spent_idx)}{product_total_row_idx+1}/IF(AND({xl_col_to_name(total_purchases_idx)}{product_total_row_idx+1}=0,{xl_col_to_name(total_amount_spent_idx)}{product_total_row_idx+1}>0),1,{xl_col_to_name(total_purchases_idx)}{product_total_row_idx+1}))",
                            product_total_format
                        )
                    elif metric == "Score":
                        # FIXED SCORE CALCULATION FOR PRODUCT TOTAL (using aggregated total values)
                        total_avg_price_idx = all_columns.index("Total_Avg Price")
                        total_purchases_idx = all_columns.index("Total_Purchases")
                        total_amount_spent_idx = all_columns.index("Total_Amount Spent (USD)")
                        total_delivery_rate_idx = all_columns.index("Total_Delivery Rate")
                        
                        avg_price_ref = f"{xl_col_to_name(total_avg_price_idx)}{product_total_row_idx+1}"
                        purchases_ref = f"{xl_col_to_name(total_purchases_idx)}{product_total_row_idx+1}"
                        amount_spent_ref = f"{xl_col_to_name(total_amount_spent_idx)}{product_total_row_idx+1}"
                        delivery_rate_ref = f"{xl_col_to_name(total_delivery_rate_idx)}{product_total_row_idx+1}"
                        
                        rate_term = f"IF(ISNUMBER({delivery_rate_ref}),IF({delivery_rate_ref}>1,{delivery_rate_ref}/100,{delivery_rate_ref}),0)"
                        
                        # FIXED: Use the pre-calculated product-level weighted average cost input
                        avg_product_cost = product_total_cost_inputs.get(product, 0)
                        
                        score_formula = f'''=IF(AND({avg_price_ref}>0,OR({purchases_ref}>0,AND({purchases_ref}=0,{amount_spent_ref}>0))),
                              (({avg_price_ref}*IF(AND({purchases_ref}=0,{amount_spent_ref}>0),1,{purchases_ref})*{rate_term})
                               -({amount_spent_ref}*100)-(77*IF(AND({purchases_ref}=0,{amount_spent_ref}>0),1,{purchases_ref}))-(65*IF(AND({purchases_ref}=0,{amount_spent_ref}>0),1,{purchases_ref}))
                               -({avg_product_cost}*IF(AND({purchases_ref}=0,{amount_spent_ref}>0),1,{purchases_ref})*{rate_term}))
                                /(({avg_price_ref}*IF(AND({purchases_ref}=0,{amount_spent_ref}>0),1,{purchases_ref})*{rate_term})*0.1),0)'''
                        
                        worksheet.write_formula(
                            product_total_row_idx, col_idx,
                            score_formula,
                            product_total_format
                        )
                    else:
                        # Sum for other metrics using ranges
                        col_range = f"{xl_col_to_name(col_idx)}{first_campaign_row}:{xl_col_to_name(col_idx)}{last_campaign_row}"
                        worksheet.write_formula(
                            product_total_row_idx, col_idx,
                            f"=SUM({col_range})",
                            product_total_format
                        )
                
                # Calculate base columns for product total (link to total columns)
                total_amount_spent_col_idx = all_columns.index("Total_Amount Spent (USD)")
                total_purchases_col_idx = all_columns.index("Total_Purchases")
                total_cost_per_purchase_col_idx = all_columns.index("Total_C.P.P (USD)")
                
                worksheet.write_formula(
                    product_total_row_idx, 2,
                    f"={xl_col_to_name(total_amount_spent_col_idx)}{product_total_row_idx+1}",
                    product_total_format
                )
                
                worksheet.write_formula(
                    product_total_row_idx, 3,
                    f"={xl_col_to_name(total_purchases_col_idx)}{product_total_row_idx+1}",
                    product_total_format
                )
                
                worksheet.write_formula(
                    product_total_row_idx, 4,
                    f"={xl_col_to_name(total_cost_per_purchase_col_idx)}{product_total_row_idx+1}",
                    product_total_format
                )
                
                # B.E (Break Even) for product total - CALCULATE ONCE FOR PRODUCT (FIXED for zero purchases)
                total_avg_price_col_idx = all_columns.index("Total_Avg Price")
                total_delivery_rate_col_idx = all_columns.index("Total_Delivery Rate")
                
                total_avg_price_ref = f"{xl_col_to_name(total_avg_price_col_idx)}{product_total_row_idx+1}"
                total_purchases_ref = f"{xl_col_to_name(total_purchases_col_idx)}{product_total_row_idx+1}"
                total_delivery_rate_ref = f"{xl_col_to_name(total_delivery_rate_col_idx)}{product_total_row_idx+1}"
                
                # FIXED: Use the pre-calculated product-level weighted average cost input
                avg_product_cost = product_total_cost_inputs.get(product, 0)
                
                break_even_formula = f'''=IF(AND({total_avg_price_ref}>0,OR({total_purchases_ref}>0,AND({total_purchases_ref}=0,{xl_col_to_name(total_amount_spent_col_idx)}{product_total_row_idx+1}>0))),
                    (({total_avg_price_ref}*IF(AND({total_purchases_ref}=0,{xl_col_to_name(total_amount_spent_col_idx)}{product_total_row_idx+1}>0),1,{total_purchases_ref})*IF(ISNUMBER({total_delivery_rate_ref}),IF({total_delivery_rate_ref}>1,{total_delivery_rate_ref}/100,{total_delivery_rate_ref}),0))
                    -(77*IF(AND({total_purchases_ref}=0,{xl_col_to_name(total_amount_spent_col_idx)}{product_total_row_idx+1}>0),1,{total_purchases_ref}))-(65*IF(AND({total_purchases_ref}=0,{xl_col_to_name(total_amount_spent_col_idx)}{product_total_row_idx+1}>0),1,{total_purchases_ref}))
                    -({avg_product_cost}*IF(AND({total_purchases_ref}=0,{xl_col_to_name(total_amount_spent_col_idx)}{product_total_row_idx+1}>0),1,{total_purchases_ref})*IF(ISNUMBER({total_delivery_rate_ref}),IF({total_delivery_rate_ref}>1,{total_delivery_rate_ref}/100,{total_delivery_rate_ref}),0)))/100/IF(AND({total_purchases_ref}=0,{xl_col_to_name(total_amount_spent_col_idx)}{product_total_row_idx+1}>0),1,{total_purchases_ref}),0)'''
                
                worksheet.write_formula(
                    product_total_row_idx, 5,
                    break_even_formula,
                    product_total_format
                )

        # Calculate grand totals using INDIVIDUAL PRODUCT TOTAL ROWS ONLY (ONLY VALID PRODUCTS)
        if product_total_rows:
            # Add base columns to grand total
            total_amount_spent_col_idx = all_columns.index("Total_Amount Spent (USD)")
            total_purchases_col_idx = all_columns.index("Total_Purchases")
            total_cost_per_purchase_col_idx = all_columns.index("Total_C.P.P (USD)")
            
            worksheet.write_formula(
                grand_total_row_idx, 2,
                f"={xl_col_to_name(total_amount_spent_col_idx)}{grand_total_row_idx+1}",
                grand_total_format
            )
            
            worksheet.write_formula(
                grand_total_row_idx, 3,
                f"={xl_col_to_name(total_purchases_col_idx)}{grand_total_row_idx+1}",
                grand_total_format
            )
            
            worksheet.write_formula(
                grand_total_row_idx, 4,
                f"={xl_col_to_name(total_cost_per_purchase_col_idx)}{grand_total_row_idx+1}",
                grand_total_format
            )
            
            # B.E (Break Even) for grand total - CALCULATE ONCE FOR GRAND TOTAL (FIXED for zero purchases)
            total_avg_price_col_idx = all_columns.index("Total_Avg Price")
            total_delivery_rate_col_idx = all_columns.index("Total_Delivery Rate")
            
            total_avg_price_ref = f"{xl_col_to_name(total_avg_price_col_idx)}{grand_total_row_idx+1}"
            total_purchases_ref = f"{xl_col_to_name(total_purchases_col_idx)}{grand_total_row_idx+1}"
            total_delivery_rate_ref = f"{xl_col_to_name(total_delivery_rate_col_idx)}{grand_total_row_idx+1}"
            
            # FIXED: Calculate weighted average product cost across all VALID products using CORRECT product-to-row mapping
            product_cost_terms = []
            product_purchases_terms = []
            for product, _ in valid_products:
                product_cost_input = product_total_cost_inputs.get(product, 0)
                # FIXED: Use the correct row index for this specific product
                product_row_idx = product_to_row_mapping.get(product)
                if product_row_idx is not None:
                    product_excel_row = product_row_idx + 1
                    product_purchases_ref = f"{xl_col_to_name(total_purchases_col_idx)}{product_excel_row}"
                    product_cost_terms.append(f"{product_cost_input}*{product_purchases_ref}")
                    product_purchases_terms.append(product_purchases_ref)
            
            # Create the weighted average formula for grand total cost input
            if product_cost_terms and product_purchases_terms:
                sumproduct_formula = "+".join(product_cost_terms)
                sum_purchases_formula = "+".join(product_purchases_terms)
                grand_avg_product_cost_formula = f"IF(({sum_purchases_formula})=0,0,({sumproduct_formula})/({sum_purchases_formula}))"
            else:
                grand_avg_product_cost_formula = "0"
            
            break_even_formula = f'''=IF(AND({total_avg_price_ref}>0,OR({total_purchases_ref}>0,AND({total_purchases_ref}=0,{xl_col_to_name(total_amount_spent_col_idx)}{grand_total_row_idx+1}>0))),
                (({total_avg_price_ref}*IF(AND({total_purchases_ref}=0,{xl_col_to_name(total_amount_spent_col_idx)}{grand_total_row_idx+1}>0),1,{total_purchases_ref})*IF(ISNUMBER({total_delivery_rate_ref}),IF({total_delivery_rate_ref}>1,{total_delivery_rate_ref}/100,{total_delivery_rate_ref}),0))
                -(77*IF(AND({total_purchases_ref}=0,{xl_col_to_name(total_amount_spent_col_idx)}{grand_total_row_idx+1}>0),1,{total_purchases_ref}))-(65*IF(AND({total_purchases_ref}=0,{xl_col_to_name(total_amount_spent_col_idx)}{grand_total_row_idx+1}>0),1,{total_purchases_ref}))
                -(({grand_avg_product_cost_formula})*IF(AND({total_purchases_ref}=0,{xl_col_to_name(total_amount_spent_col_idx)}{grand_total_row_idx+1}>0),1,{total_purchases_ref})*IF(ISNUMBER({total_delivery_rate_ref}),IF({total_delivery_rate_ref}>1,{total_delivery_rate_ref}/100,{total_delivery_rate_ref}),0)))/100/IF(AND({total_purchases_ref}=0,{xl_col_to_name(total_amount_spent_col_idx)}{grand_total_row_idx+1}>0),1,{total_purchases_ref}),0)'''
            
            worksheet.write_formula(
                grand_total_row_idx, 5,
                break_even_formula,
                grand_total_format
            )
            
            # Date-specific and total columns for grand total using INDIVIDUAL PRODUCT ROWS (ONLY VALID PRODUCTS)
            for date in unique_dates:
                for metric in date_metrics:
                    col_idx = all_columns.index(f"{date}_{metric}")
                    
                    if metric == "Delivery status":
                        # For Delivery status in grand total, leave empty (not applicable)
                        safe_write(worksheet, grand_total_row_idx, col_idx, "", grand_total_format)
                    
                    elif metric in ["Avg Price", "Delivery Rate"]:
                        # Weighted average using individual product total rows
                        date_purchases_col_idx = all_columns.index(f"{date}_Purchases")
                        
                        metric_refs = []
                        purchases_refs = []
                        for product_row_idx in product_total_rows:
                            product_excel_row = product_row_idx + 1
                            metric_refs.append(f"{xl_col_to_name(col_idx)}{product_excel_row}")
                            purchases_refs.append(f"{xl_col_to_name(date_purchases_col_idx)}{product_excel_row}")
                        
                        # Build SUMPRODUCT formula for weighted average
                        sumproduct_terms = []
                        for i in range(len(metric_refs)):
                            sumproduct_terms.append(f"{metric_refs[i]}*{purchases_refs[i]}")
                        
                        sumproduct_formula = "+".join(sumproduct_terms)
                        sum_purchases_formula = "+".join(purchases_refs)
                        
                        worksheet.write_formula(
                            grand_total_row_idx, col_idx,
                            f"=IF(({sum_purchases_formula})=0,0,({sumproduct_formula})/({sum_purchases_formula}))",
                            grand_total_format
                        )
                    elif metric == "C.P.P (USD)":  # FIXED for zero purchases
                        # Calculate based on totals for this date
                        amount_spent_idx = all_columns.index(f"{date}_Amount Spent (USD)")
                        purchases_idx = all_columns.index(f"{date}_Purchases")
                        worksheet.write_formula(
                            grand_total_row_idx, col_idx,
                            f"=IF(AND({xl_col_to_name(purchases_idx)}{grand_total_row_idx+1}=0,{xl_col_to_name(amount_spent_idx)}{grand_total_row_idx+1}=0),0,{xl_col_to_name(amount_spent_idx)}{grand_total_row_idx+1}/IF(AND({xl_col_to_name(purchases_idx)}{grand_total_row_idx+1}=0,{xl_col_to_name(amount_spent_idx)}{grand_total_row_idx+1}>0),1,{xl_col_to_name(purchases_idx)}{grand_total_row_idx+1}))",
                            grand_total_format
                        )
                    elif metric == "Score":
                        # FIXED SCORE CALCULATION FOR GRAND TOTAL (using aggregated values)
                        avg_price_idx = all_columns.index(f"{date}_Avg Price")
                        purchases_idx = all_columns.index(f"{date}_Purchases")
                        amount_spent_idx = all_columns.index(f"{date}_Amount Spent (USD)")
                        delivery_rate_idx = all_columns.index(f"{date}_Delivery Rate")
                        
                        avg_price_ref = f"{xl_col_to_name(avg_price_idx)}{grand_total_row_idx+1}"
                        purchases_ref = f"{xl_col_to_name(purchases_idx)}{grand_total_row_idx+1}"
                        amount_spent_ref = f"{xl_col_to_name(amount_spent_idx)}{grand_total_row_idx+1}"
                        delivery_rate_ref = f"{xl_col_to_name(delivery_rate_idx)}{grand_total_row_idx+1}"
                        
                        rate_term = f"IF(ISNUMBER({delivery_rate_ref}),IF({delivery_rate_ref}>1,{delivery_rate_ref}/100,{delivery_rate_ref}),0)"
                        
                        # FIXED: Calculate weighted average product cost for this specific date using CORRECT product-to-row mapping
                        date_product_cost_terms = []
                        date_purchases_terms = []
                        for product, _ in valid_products:
                            date_cost_input = product_date_cost_inputs.get(product, {}).get(date, 0)
                            # FIXED: Use the correct row index for this specific product
                            product_row_idx = product_to_row_mapping.get(product)
                            if product_row_idx is not None:
                                product_excel_row = product_row_idx + 1
                                date_purchases_ref = f"{xl_col_to_name(purchases_idx)}{product_excel_row}"
                                date_product_cost_terms.append(f"{date_cost_input}*{date_purchases_ref}")
                                date_purchases_terms.append(date_purchases_ref)
                        
                        if date_product_cost_terms and date_purchases_terms:
                            date_sumproduct_formula = "+".join(date_product_cost_terms)
                            date_sum_purchases_formula = "+".join(date_purchases_terms)
                            date_avg_product_cost_formula = f"IF(({date_sum_purchases_formula})=0,0,({date_sumproduct_formula})/({date_sum_purchases_formula}))"
                        else:
                            date_avg_product_cost_formula = "0"
                        
                        score_formula = f'''=IF(AND({avg_price_ref}>0,OR({purchases_ref}>0,AND({purchases_ref}=0,{amount_spent_ref}>0))),
                            (({avg_price_ref}*IF(AND({purchases_ref}=0,{amount_spent_ref}>0),1,{purchases_ref})*{rate_term})
                            -({amount_spent_ref}*100)-(77*IF(AND({purchases_ref}=0,{amount_spent_ref}>0),1,{purchases_ref}))-(65*IF(AND({purchases_ref}=0,{amount_spent_ref}>0),1,{purchases_ref}))
                            -(({date_avg_product_cost_formula})*IF(AND({purchases_ref}=0,{amount_spent_ref}>0),1,{purchases_ref})*{rate_term}))
                            /(({avg_price_ref}*IF(AND({purchases_ref}=0,{amount_spent_ref}>0),1,{purchases_ref})*{rate_term})*0.1),0)'''
                        
                        worksheet.write_formula(
                            grand_total_row_idx, col_idx,
                            score_formula,
                            grand_total_format
                        )
                    else:
                        # Sum using individual product total rows only
                        sum_refs = []
                        for product_row_idx in product_total_rows:
                            product_excel_row = product_row_idx + 1
                            sum_refs.append(f"{xl_col_to_name(col_idx)}{product_excel_row}")
                        
                        sum_formula = "+".join(sum_refs)
                        worksheet.write_formula(
                            grand_total_row_idx, col_idx,
                            f"={sum_formula}",
                            grand_total_format
                        )
            
            # Total columns for grand total using INDIVIDUAL PRODUCT TOTAL ROWS (ONLY VALID PRODUCTS)
            total_purchases_col_idx = all_columns.index("Total_Purchases")
            
            for metric in date_metrics:
                col_idx = all_columns.index(f"Total_{metric}")
                
                if metric == "Delivery status":
                    # For Delivery status in grand total, leave empty (not applicable)
                    safe_write(worksheet, grand_total_row_idx, col_idx, "", grand_total_format)
                
                elif metric in ["Avg Price", "Delivery Rate"]:
                    # Weighted average using individual product total rows
                    metric_refs = []
                    purchases_refs = []
                    for product_row_idx in product_total_rows:
                        product_excel_row = product_row_idx + 1
                        metric_refs.append(f"{xl_col_to_name(col_idx)}{product_excel_row}")
                        purchases_refs.append(f"{xl_col_to_name(total_purchases_col_idx)}{product_excel_row}")
                    
                    # Build SUMPRODUCT formula for weighted average
                    sumproduct_terms = []
                    for i in range(len(metric_refs)):
                        sumproduct_terms.append(f"{metric_refs[i]}*{purchases_refs[i]}")
                    
                    sumproduct_formula = "+".join(sumproduct_terms)
                    sum_purchases_formula = "+".join(purchases_refs)
                    
                    worksheet.write_formula(
                        grand_total_row_idx, col_idx,
                        f"=IF(({sum_purchases_formula})=0,0,({sumproduct_formula})/({sum_purchases_formula}))",
                        grand_total_format
                    )
                elif metric == "C.P.P (USD)":  # FIXED for zero purchases
                    # Calculate based on totals
                    total_amount_spent_idx = all_columns.index("Total_Amount Spent (USD)")
                    total_purchases_idx = all_columns.index("Total_Purchases")
                    worksheet.write_formula(
                        grand_total_row_idx, col_idx,
                        f"=IF(AND({xl_col_to_name(total_purchases_idx)}{grand_total_row_idx+1}=0,{xl_col_to_name(total_amount_spent_idx)}{grand_total_row_idx+1}=0),0,{xl_col_to_name(total_amount_spent_idx)}{grand_total_row_idx+1}/IF(AND({xl_col_to_name(total_purchases_idx)}{grand_total_row_idx+1}=0,{xl_col_to_name(total_amount_spent_idx)}{grand_total_row_idx+1}>0),1,{xl_col_to_name(total_purchases_idx)}{grand_total_row_idx+1}))",
                        grand_total_format
                    )
                elif metric == "Score":
                    # FIXED SCORE CALCULATION FOR GRAND TOTAL (using aggregated total values)
                    total_avg_price_idx = round(all_columns.index("Total_Avg Price"),2)
                    total_purchases_idx = round(all_columns.index("Total_Purchases"),2)
                    total_amount_spent_idx = round(all_columns.index("Total_Amount Spent (USD)"),2)
                    total_delivery_rate_idx = round(all_columns.index("Total_Delivery Rate"),2)
                    
                    avg_price_ref = f"{xl_col_to_name(total_avg_price_idx)}{grand_total_row_idx+1}"
                    purchases_ref = f"{xl_col_to_name(total_purchases_idx)}{grand_total_row_idx+1}"
                    amount_spent_ref = f"{xl_col_to_name(total_amount_spent_idx)}{grand_total_row_idx+1}"
                    delivery_rate_ref = f"{xl_col_to_name(total_delivery_rate_idx)}{grand_total_row_idx+1}"
                    
                    rate_term = f"IF(ISNUMBER({delivery_rate_ref}),IF({delivery_rate_ref}>1,{delivery_rate_ref}/100,{delivery_rate_ref}),0)"
                    
                    # FIXED: Use the same weighted average product cost formula as calculated above for grand total
                    score_formula = f'''=IF(AND({avg_price_ref}>0,OR({purchases_ref}>0,AND({purchases_ref}=0,{amount_spent_ref}>0))),
                        (({avg_price_ref}*IF(AND({purchases_ref}=0,{amount_spent_ref}>0),1,{purchases_ref})*{rate_term})
                        -({amount_spent_ref}*100)-(77*IF(AND({purchases_ref}=0,{amount_spent_ref}>0),1,{purchases_ref}))-(65*IF(AND({purchases_ref}=0,{amount_spent_ref}>0),1,{purchases_ref}))
                        -(({grand_avg_product_cost_formula})*IF(AND({purchases_ref}=0,{amount_spent_ref}>0),1,{purchases_ref})*{rate_term}))
                        /(({avg_price_ref}*IF(AND({purchases_ref}=0,{amount_spent_ref}>0),1,{purchases_ref})*{rate_term})*0.1),0)'''
                    
                    worksheet.write_formula(
                        grand_total_row_idx, col_idx,
                        score_formula,
                        grand_total_format
                    )
                else:
                    # Sum using individual product total rows only
                    sum_refs = []
                    for product_row_idx in product_total_rows:
                        product_excel_row = product_row_idx + 1
                        sum_refs.append(f"{xl_col_to_name(col_idx)}{product_excel_row}")
                    
                    sum_formula = "+".join(sum_refs)
                    worksheet.write_formula(
                        grand_total_row_idx, col_idx,
                        f"={sum_formula}",
                        grand_total_format
                    )

        # NEW: Add excluded products table at the end of the sheet
        if excluded_products:
            # Add some spacing
            exclusion_start_row = row + 3
            
            # Title for exclusion table
            safe_write(worksheet, exclusion_start_row, 0, "PRODUCTS EXCLUDED FROM CALCULATIONS", exclusion_header_format)
            safe_write(worksheet, exclusion_start_row + 1, 0, "These products have product cost input = 0 and delivery rate = 0", exclusion_data_format)
            
            # Headers for exclusion table
            exclusion_headers = ["Product Name", "Campaign Count", "Total Amount Spent (USD)", "Total Purchases", "Reason"]
            exclusion_header_row = exclusion_start_row + 3
            
            for col_num, header in enumerate(exclusion_headers):
                safe_write(worksheet, exclusion_header_row, col_num, header, exclusion_header_format)
            
            # Write excluded products data
            current_exclusion_row = exclusion_header_row + 1
            for excluded_product in excluded_products:
                safe_write(worksheet, current_exclusion_row, 0, excluded_product['Product'], exclusion_data_format)
                safe_write(worksheet, current_exclusion_row, 1, excluded_product['Campaign Count'], exclusion_data_format)
                safe_write(worksheet, current_exclusion_row, 2, excluded_product['Total Amount Spent (USD)'], exclusion_data_format)
                safe_write(worksheet, current_exclusion_row, 3, excluded_product['Total Purchases'], exclusion_data_format)
                safe_write(worksheet, current_exclusion_row, 4, excluded_product['Reason'], exclusion_data_format)
                current_exclusion_row += 1
            
            # Add summary for excluded products
            summary_row = current_exclusion_row + 1
            safe_write(worksheet, summary_row, 0, "EXCLUSION SUMMARY", exclusion_header_format)
            safe_write(worksheet, summary_row + 1, 0, f"Total excluded products: {len(excluded_products)}", exclusion_data_format)
            
            total_excluded_amount = sum(p['Total Amount Spent (USD)'] for p in excluded_products)
            total_excluded_purchases = sum(p['Total Purchases'] for p in excluded_products)
            total_excluded_campaigns = sum(p['Campaign Count'] for p in excluded_products)
            
            safe_write(worksheet, summary_row + 2, 0, f"Total excluded amount spent: ${total_excluded_amount:,.2f}", exclusion_data_format)
            safe_write(worksheet, summary_row + 3, 0, f"Total excluded purchases: {total_excluded_purchases:,}", exclusion_data_format)
            safe_write(worksheet, summary_row + 4, 0, f"Total excluded campaigns: {total_excluded_campaigns}", exclusion_data_format)

        # Freeze panes to keep base columns visible when scrolling
        worksheet.freeze_panes(2, len(base_columns))
        
        
        # ==== UNMATCHED CAMPAIGNS SHEET ====
        unmatched_sheet = workbook.add_worksheet("Unmatched Campaigns")
        
        # Formats for unmatched sheet
        unmatched_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#FF9999", "font_name": "Calibri", "font_size": 11
        })
        unmatched_data_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#FFE6E6", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        matched_summary_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#E6FFE6", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        
        # Headers for unmatched sheet (UPDATED: changed "Cost Per Purchase (USD)" to "C.P.P (USD)")
        unmatched_headers = ["Status", "Product", "Campaign Name", "Amount Spent (USD)", 
                           "Purchases", "C.P.P (USD)", "Dates Covered", "Reason"]
        
        for col_num, header in enumerate(unmatched_headers):
            safe_write(unmatched_sheet, 0, col_num, header, unmatched_header_format)
        
        # Write summary first
        summary_row = 1
        safe_write(unmatched_sheet, summary_row, 0, "SUMMARY", unmatched_header_format)
        safe_write(unmatched_sheet, summary_row + 1, 0, f"Total Campaigns: {len(matched_campaigns) + len(unmatched_campaigns)}", matched_summary_format)
        safe_write(unmatched_sheet, summary_row + 2, 0, f"Matched with Shopify: {len(matched_campaigns)}", matched_summary_format)
        safe_write(unmatched_sheet, summary_row + 3, 0, f"Unmatched with Shopify: {len(unmatched_campaigns)}", unmatched_data_format)
        safe_write(unmatched_sheet, summary_row + 4, 0, f"Date Range: {min(unique_dates)} to {max(unique_dates)}" if unique_dates else "No dates found", matched_summary_format)
        
        # Write unmatched campaigns
        current_row = summary_row + 6
        
        if unmatched_campaigns:
            safe_write(unmatched_sheet, current_row, 0, "CAMPAIGNS WITHOUT SHOPIFY DATA", unmatched_header_format)
            current_row += 1
            
            for campaign in unmatched_campaigns:
                # MODIFIED CPP calculation for unmatched campaigns sheet
                cost_per_purchase_usd = 0
                if campaign['Amount Spent (USD)'] > 0 and campaign['Purchases'] == 0:
                    cost_per_purchase_usd = round(campaign['Amount Spent (USD)'] / 1, 2)  # Use 1 when no purchases but has spending
                elif campaign['Purchases'] > 0:
                    cost_per_purchase_usd = round(campaign['Amount Spent (USD)'] / campaign['Purchases'], 2)
                
                dates_str = ", ".join(campaign['Dates']) if campaign['Dates'] else "No dates"
                
                safe_write(unmatched_sheet, current_row, 0, "UNMATCHED", unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 1, campaign['Product'], unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 2, campaign['Campaign Name'], unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 3, campaign['Amount Spent (USD)'], unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 4, campaign['Purchases'], unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 5, cost_per_purchase_usd, unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 6, dates_str, unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 7, "No matching Shopify day-wise data found", unmatched_data_format)
                current_row += 1
        
        # Write matched campaigns summary
        if matched_campaigns:
            current_row += 2
            safe_write(unmatched_sheet, current_row, 0, "CAMPAIGNS WITH SHOPIFY DATA (FOR REFERENCE)", unmatched_header_format)
            current_row += 1
            
            for campaign in matched_campaigns[:10]:  # Show only first 10 to save space
                # MODIFIED CPP calculation for matched campaigns sheet
                cost_per_purchase_usd = 0
                if campaign['Amount Spent (USD)'] > 0 and campaign['Purchases'] == 0:
                    cost_per_purchase_usd = round(campaign['Amount Spent (USD)'] / 1, 2)  # Use 1 when no purchases but has spending
                elif campaign['Purchases'] > 0:
                    cost_per_purchase_usd = round(campaign['Amount Spent (USD)'] / campaign['Purchases'], 2)
                
                dates_str = ", ".join(campaign['Dates']) if campaign['Dates'] else "No dates"
                
                safe_write(unmatched_sheet, current_row, 0, "MATCHED", matched_summary_format)
                safe_write(unmatched_sheet, current_row, 1, campaign['Product'], matched_summary_format)
                safe_write(unmatched_sheet, current_row, 2, campaign['Campaign Name'], matched_summary_format)
                safe_write(unmatched_sheet, current_row, 3, campaign['Amount Spent (USD)'], matched_summary_format)
                safe_write(unmatched_sheet, current_row, 4, campaign['Purchases'], matched_summary_format)
                safe_write(unmatched_sheet, current_row, 5, cost_per_purchase_usd, matched_summary_format)
                safe_write(unmatched_sheet, current_row, 6, dates_str, matched_summary_format)
                safe_write(unmatched_sheet, current_row, 7, "Successfully matched with Shopify day-wise data", matched_summary_format)
                current_row += 1
            
            if len(matched_campaigns) > 10:
                safe_write(unmatched_sheet, current_row, 0, f"... and {len(matched_campaigns) - 10} more matched campaigns", matched_summary_format)
        
        # Set column widths for unmatched sheet
        unmatched_sheet.set_column(0, 0, 12)  # Status
        unmatched_sheet.set_column(1, 1, 25)  # Product
        unmatched_sheet.set_column(2, 2, 35)  # Campaign Name
        unmatched_sheet.set_column(3, 3, 18)  # Amount USD
        unmatched_sheet.set_column(4, 4, 12)  # Purchases
        unmatched_sheet.set_column(5, 5, 20)  # C.P.P USD
        unmatched_sheet.set_column(6, 6, 25)  # Dates Covered
        unmatched_sheet.set_column(7, 7, 40)  # Reason

        # ==== NEGATIVE SCORE CAMPAIGNS SHEET (using only valid products) ====
        negative_score_sheet = workbook.add_worksheet("Negative Score Campaigns")

        # Formats for negative score sheet
        severe_negative_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#FF6B6B", "font_name": "Calibri", "font_size": 11
        })
        severe_negative_data_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#FFE6E6", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        moderate_negative_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#FFA500", "font_name": "Calibri", "font_size": 11
        })
        moderate_negative_data_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#FFE4B5", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        positive_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#ABEA53", "font_name": "Calibri", "font_size": 11
        })
        positive_data_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#F0FFF0", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        last_date_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#559BD8", "font_name": "Calibri", "font_size": 11
        })
        last_date_data_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#E6E6FA", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })

        # Helper function to format dates
        def format_date_readable(date_str):
            """Convert date string to more readable format like '9th September 2025'"""
            try:
                from datetime import datetime
                for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y']:
                    try:
                        date_obj = datetime.strptime(date_str, fmt)
                        day = date_obj.day
                        if 4 <= day <= 20 or 24 <= day <= 30:
                            suffix = "th"
                        else:
                            suffix = ["st", "nd", "rd"][day % 10 - 1]
                        return f"{day}{suffix} {date_obj.strftime('%B %Y')}"
                    except ValueError:
                        continue
                return date_str
            except:
                return date_str

        # Calculate selected_days threshold
        if selected_days is None:
            selected_days = max(1, len(unique_dates) // 2)

        # Analyze campaigns for negative scores (ONLY FROM VALID PRODUCTS)
        all_negative_campaigns = []

        # Group by product and campaign to analyze scores (ONLY VALID PRODUCTS)
        for product, product_df in valid_products:
            for campaign_name, campaign_group in product_df.groupby("Campaign Name"):
                campaign_dates = sorted([str(d) for d in campaign_group['Date'].unique() if pd.notna(d) and str(d).strip() != ''])
                
                if len(campaign_dates) < selected_days:
                    continue
                
                # Calculate CPP and Amount Spent
                total_amount_spent_usd = campaign_group.get("Amount Spent (USD)", 0).sum() if "Amount Spent (USD)" in campaign_group.columns else 0
                total_purchases = campaign_group.get("Purchases", 0).sum() if "Purchases" in campaign_group.columns else 0
                
                cpp = 0
                if total_amount_spent_usd > 0 and total_purchases == 0:
                    cpp = total_amount_spent_usd / 1
                elif total_purchases > 0:
                    cpp = total_amount_spent_usd / total_purchases
                
                # Calculate B.E
                product_costs = []
                for date in unique_dates:
                    date_cost = product_date_cost_inputs.get(product, {}).get(date, 0)
                    if date_cost > 0:
                        product_costs.append(date_cost)
                avg_product_cost = sum(product_costs) / len(product_costs) if product_costs else 0
                
                product_avg_price = product_total_avg_prices.get(product, 0)
                product_delivery_rate = product_total_delivery_rates.get(product, 0)
                
                be = 0
                if product_avg_price > 0 and (total_purchases > 0 or (total_purchases == 0 and total_amount_spent_usd > 0)):
                    calc_purchases = 1 if (total_purchases == 0 and total_amount_spent_usd > 0) else total_purchases
                    delivery_rate = product_delivery_rate / 100 if product_delivery_rate > 1 else product_delivery_rate
                    revenue = product_avg_price * calc_purchases * delivery_rate
                    costs = (77 * calc_purchases) + (65 * calc_purchases) + (avg_product_cost * calc_purchases * delivery_rate)
                    if calc_purchases > 0:
                        be = (revenue - costs) / 100 / calc_purchases

                # Calculate overall Score
                # Calculate overall Score using day-by-day method (matching admin's net profit % calculation)
                overall_score = 0
                total_net_profit_sum = 0  # Sum of day-by-day net profits
                campaign_dates = sorted([str(d) for d in campaign_group['Date'].unique() if pd.notna(d) and str(d).strip() != ''])

                if product_avg_price > 0:
    # Calculate net profit by summing day-by-day (matching admin file)
                    for date in campaign_dates:
                       date_data = campaign_group[campaign_group['Date'].astype(str) == date]
                       if not date_data.empty:
                           row_data = date_data.iloc[0]
            
            # Get day-specific data
                           date_amount_spent = round(row_data.get("Amount Spent (USD)", 0) if pd.notna(row_data.get("Amount Spent (USD)")) else 0, 2)
                           date_purchases = round(row_data.get("Purchases", 0) if pd.notna(row_data.get("Purchases")) else 0, 2)
            
            # Get day-wise lookup data
                           date_avg_price = round(product_date_avg_prices.get(product, {}).get(date, 0), 2)
                           date_delivery_rate = round(product_date_delivery_rates.get(product, {}).get(date, 0), 2)
                           date_product_cost = round(product_date_cost_inputs.get(product, {}).get(date, 0), 2)
            
            # Calculate for this specific date (SAME AS ADMIN FILE)
                           calc_purchases_date = round(date_purchases, 2)
                           delivery_rate_date = round(date_delivery_rate / 100 if date_delivery_rate > 1 else date_delivery_rate, 2)
            
                           delivered_orders = round(calc_purchases_date * delivery_rate_date, 2)
                           net_revenue = round(delivered_orders * date_avg_price, 2)
                           total_product_cost_date = round(delivered_orders * date_product_cost, 2)
                           total_shipping_cost_date = round(calc_purchases_date * 77, 2)
                           total_operational_cost_date = round(calc_purchases_date * 65, 2)
            
            # Net profit for THIS DATE
                           date_net_profit = round(net_revenue - (date_amount_spent * 100) - total_shipping_cost_date - total_operational_cost_date - total_product_cost_date, 2)
            
            # ADD to total
                           total_net_profit_sum += round(date_net_profit, 2)
    
    # Now calculate Score = Net Profit % / 10
                    calc_purchases_total = 1 if (total_purchases == 0 and total_amount_spent_usd > 0) else total_purchases
                    delivery_rate_total = round(product_delivery_rate / 100 if product_delivery_rate > 1 else product_delivery_rate, 2)
    
                    numerator_total = round(total_net_profit_sum, 2)
                    denominator_total = round(product_avg_price * calc_purchases_total * delivery_rate_total, 2)
                    net_profit_percentage = round((numerator_total / denominator_total * 100), 2) if denominator_total > 0 else 0
    
    # Score = Net Profit % / 10
                    overall_score = round(net_profit_percentage / 10, 2)
                
                # Calculate scores for ALL dates
                date_scores = []
                for date in campaign_dates:
                    date_data = campaign_group[campaign_group['Date'].astype(str) == date]
                    if not date_data.empty:
                        row_data = date_data.iloc[0]
                        amount_spent = row_data.get("Amount Spent (USD)", 0) or 0
                        purchases = row_data.get("Purchases", 0) or 0
                        
                        date_avg_price = product_date_avg_prices.get(product, {}).get(date, 0)
                        date_delivery_rate = product_date_delivery_rates.get(product, {}).get(date, 0)
                        date_product_cost = product_date_cost_inputs.get(product, {}).get(date, 0)
                        
                        if date_avg_price > 0 and (purchases > 0 or (purchases == 0 and amount_spent > 0)):
                            calc_purchases = 1 if (purchases == 0 and amount_spent > 0) else purchases
                            delivery_rate = date_delivery_rate / 100 if date_delivery_rate > 1 else date_delivery_rate
                            revenue = date_avg_price * calc_purchases * delivery_rate
                            costs = (amount_spent * 100) + (77 * calc_purchases) + (65 * calc_purchases) + (date_product_cost * calc_purchases * delivery_rate)
                            profit = revenue - costs
                            score = (profit / (revenue * 0.1)) if revenue > 0 else 0
                            
                            date_scores.append({
                                'date': date,
                                'score': score,
                                'amount_spent': amount_spent,
                                'purchases': purchases
                            })
                
                # Count negative scores
                negative_score_data = [data for data in date_scores if data['score'] < 0]
                
                if len(negative_score_data) >= selected_days:
                    negative_dates = [data['date'] for data in negative_score_data]
                    formatted_negative_dates = [format_date_readable(date) for date in negative_dates[:10]]
                    formatted_dates_str = ", ".join(formatted_negative_dates)
                    if len(negative_dates) > 10:
                        formatted_dates_str += "..."
                    
                    last_date = campaign_dates[-1] if campaign_dates else None
                    last_day_delivery_status = ""

                    if last_date:
                       last_date_data = campaign_group[campaign_group['Date'].astype(str) == last_date]
                       if not last_date_data.empty:
                            row_data = last_date_data.iloc[0]
                            delivery_status_raw = row_data.get("Delivery status", "")
                            if pd.notna(delivery_status_raw) and str(delivery_status_raw).strip() != "":
                                 delivery_status_normalized = str(delivery_status_raw).strip().lower()
            # Consider "recently completed" and "inactive" as the same (Inactive)
                                 if "active" in delivery_status_normalized and "inactive" not in delivery_status_normalized:
                                       last_day_delivery_status = "Active"
                                 else:
                                       last_day_delivery_status = "Inactive"
                    negative_campaign = {
                        'Product': str(product),
                        'Campaign Name': str(campaign_name),
                        'Total Dates': len(campaign_dates),
                        'Days Checked': selected_days,
                        'Days with Negative Score': len(negative_score_data),
                        'C.P.P': round(cpp, 2),
                        'B.E': round(be, 2),
                        'Amount Spent (USD)': round(total_amount_spent_usd, 2),
                        'Score': round(overall_score, 2),
                        'Last Day Delivery Status': last_day_delivery_status,
                        'Negative Score Dates': formatted_dates_str,
                        'Reason': f"Has {len(negative_score_data)} negative score days out of {len(campaign_dates)} total days (threshold: {selected_days})"
                    }
                    
                    all_negative_campaigns.append(negative_campaign)

        # SPLIT CAMPAIGNS INTO THREE GROUPS BASED ON SCORE
        severe_negative_campaigns = []  # Score <= -1 (between -10 to -1)
        moderate_negative_campaigns = []  # Score > -1 and < 0 (between -1 to 0, excluding 0)
        positive_campaigns = []  # Score >= 0 (0 and above)
        
        for campaign in all_negative_campaigns:
            if campaign['Score'] <= -1:
                severe_negative_campaigns.append(campaign)
            elif campaign['Score'] < 0:  # > -1 and < 0
                moderate_negative_campaigns.append(campaign)
            else:  # >= 0
                positive_campaigns.append(campaign)
        
        # Sort all three groups
        severe_negative_campaigns.sort(key=lambda x: x['Score'])  # Worst first (most negative)
        moderate_negative_campaigns.sort(key=lambda x: x['Score'])  # Worst first (most negative)
        positive_campaigns.sort(key=lambda x: x['Score'], reverse=True)  # Best first (highest positive)

        # ==== PART 1: SEVERE NEGATIVE CAMPAIGNS (-10 to -1) ====
        current_row = 0
        
        # Headers for severe negative campaigns (with Comment column)
        severe_headers = ["Product", "Campaign Name", "C.P.P", "B.E", "Amount Spent (USD)", "Score","Last Day Delivery Status","Comment", "Total Dates", "Days Checked", 
                         "Days with Negative Score", "Negative Score Dates",  "Reason"]

        safe_write(negative_score_sheet, current_row, 0, "CAMPAIGNS WITH SEVERE NEGATIVE SCORE (-10 TO -1)", severe_negative_header_format)
        current_row += 1

        for col_num, header in enumerate(severe_headers):
            safe_write(negative_score_sheet, current_row, col_num, header, severe_negative_header_format)
        current_row += 1

        # Write severe negative campaigns
        if severe_negative_campaigns:
            for campaign in severe_negative_campaigns:
                # Calculate daily spend for comment
                daily_spend = campaign['Amount Spent (USD)'] / campaign['Total Dates'] if campaign['Total Dates'] > 0 else 0
                
                # Determine comment based on daily spend
                if daily_spend < 20:
                    comment = "Turn it off"
                else:
                    comment = "Change the bid and keep BE value as add cost"
                
                safe_write(negative_score_sheet, current_row, 0, campaign['Product'], severe_negative_data_format)
                safe_write(negative_score_sheet, current_row, 1, campaign['Campaign Name'], severe_negative_data_format)
                safe_write(negative_score_sheet, current_row, 2, campaign['C.P.P'], severe_negative_data_format)
                safe_write(negative_score_sheet, current_row, 3, campaign['B.E'], severe_negative_data_format)
                safe_write(negative_score_sheet, current_row, 4, campaign['Amount Spent (USD)'], severe_negative_data_format)
                safe_write(negative_score_sheet, current_row, 5, campaign['Score'], severe_negative_data_format)
                safe_write(negative_score_sheet, current_row, 6, campaign['Last Day Delivery Status'], severe_negative_data_format)
                safe_write(negative_score_sheet, current_row, 7, comment, severe_negative_data_format)
                safe_write(negative_score_sheet, current_row, 8, campaign['Total Dates'], severe_negative_data_format)
                safe_write(negative_score_sheet, current_row, 9, campaign['Days Checked'], severe_negative_data_format)
                safe_write(negative_score_sheet, current_row, 10, campaign['Days with Negative Score'], severe_negative_data_format)
                safe_write(negative_score_sheet, current_row, 11, campaign['Negative Score Dates'], severe_negative_data_format)
                safe_write(negative_score_sheet, current_row, 12, comment, severe_negative_data_format)
                safe_write(negative_score_sheet, current_row, 13, campaign['Reason'], severe_negative_data_format)
                current_row += 1
        else:
            safe_write(negative_score_sheet, current_row, 0, "No campaigns found with severe negative score (-10 to -1)", severe_negative_data_format)
            current_row += 1

        # Add summary for severe negative campaigns
        current_row += 2
        safe_write(negative_score_sheet, current_row, 0, "SUMMARY - SEVERE NEGATIVE CAMPAIGNS", severe_negative_header_format)
        safe_write(negative_score_sheet, current_row + 1, 0, f"Campaigns with severe negative score (-10 to -1): {len(severe_negative_campaigns)}", severe_negative_data_format)
        current_row += 3

        # ==== PART 2: MODERATE NEGATIVE CAMPAIGNS (-1 to 0, excluding 0) ====
        current_row += 2
        
        # Headers for moderate negative campaigns (without Comment column)
        moderate_headers = ["Product", "Campaign Name", "C.P.P", "B.E", "Amount Spent (USD)", "Score", "Total Dates", "Days Checked", 
                           "Days with Negative Score", "Negative Score Dates", "Reason"]

        safe_write(negative_score_sheet, current_row, 0, "CAMPAIGNS WITH MODERATE NEGATIVE SCORE (-1 TO 0, EXCLUDING 0)", moderate_negative_header_format)
        current_row += 1

        for col_num, header in enumerate(moderate_headers):
            safe_write(negative_score_sheet, current_row, col_num, header, moderate_negative_header_format)
        current_row += 1

        # Write moderate negative campaigns
        if moderate_negative_campaigns:
            for campaign in moderate_negative_campaigns:
                safe_write(negative_score_sheet, current_row, 0, campaign['Product'], moderate_negative_data_format)
                safe_write(negative_score_sheet, current_row, 1, campaign['Campaign Name'], moderate_negative_data_format)
                safe_write(negative_score_sheet, current_row, 2, campaign['C.P.P'], moderate_negative_data_format)
                safe_write(negative_score_sheet, current_row, 3, campaign['B.E'], moderate_negative_data_format)
                safe_write(negative_score_sheet, current_row, 4, campaign['Amount Spent (USD)'], moderate_negative_data_format)
                safe_write(negative_score_sheet, current_row, 5, campaign['Score'], moderate_negative_data_format)
                safe_write(negative_score_sheet, current_row, 6, campaign['Total Dates'], moderate_negative_data_format)
                safe_write(negative_score_sheet, current_row, 7, campaign['Days Checked'], moderate_negative_data_format)
                safe_write(negative_score_sheet, current_row, 8, campaign['Days with Negative Score'], moderate_negative_data_format)
                safe_write(negative_score_sheet, current_row, 9, campaign['Negative Score Dates'], moderate_negative_data_format)
                safe_write(negative_score_sheet, current_row, 10, campaign['Reason'], moderate_negative_data_format)
                current_row += 1
        else:
            safe_write(negative_score_sheet, current_row, 0, "No campaigns found with moderate negative score (-1 to 0, excluding 0)", moderate_negative_data_format)
            current_row += 1

        # Add summary for moderate negative campaigns
        current_row += 2
        safe_write(negative_score_sheet, current_row, 0, "SUMMARY - MODERATE NEGATIVE CAMPAIGNS", moderate_negative_header_format)
        safe_write(negative_score_sheet, current_row + 1, 0, f"Campaigns with moderate negative score (-1 to 0, excluding 0): {len(moderate_negative_campaigns)}", moderate_negative_data_format)
        
        # ==== PART 3: POSITIVE CAMPAIGNS (0 AND ABOVE) ====
        current_row += 5
        
        # Headers for positive campaigns
        positive_headers = ["Product", "Campaign Name", "C.P.P", "B.E", "Amount Spent (USD)", "Score", "Total Dates", "Days Checked", 
                           "Days with Negative Score", "Negative Score Dates", "Reason"]

        safe_write(negative_score_sheet, current_row, 0, "CAMPAIGNS WITH POSITIVE SCORE (0 AND ABOVE)", positive_header_format)
        current_row += 1

        for col_num, header in enumerate(positive_headers):
            safe_write(negative_score_sheet, current_row, col_num, header, positive_header_format)
        current_row += 1

        # Write positive campaigns
        if positive_campaigns:
            for campaign in positive_campaigns:
                safe_write(negative_score_sheet, current_row, 0, campaign['Product'], positive_data_format)
                safe_write(negative_score_sheet, current_row, 1, campaign['Campaign Name'], positive_data_format)
                safe_write(negative_score_sheet, current_row, 2, campaign['C.P.P'], positive_data_format)
                safe_write(negative_score_sheet, current_row, 3, campaign['B.E'], positive_data_format)
                safe_write(negative_score_sheet, current_row, 4, campaign['Amount Spent (USD)'], positive_data_format)
                safe_write(negative_score_sheet, current_row, 5, campaign['Score'], positive_data_format)
                safe_write(negative_score_sheet, current_row, 6, campaign['Total Dates'], positive_data_format)
                safe_write(negative_score_sheet, current_row, 7, campaign['Days Checked'], positive_data_format)
                safe_write(negative_score_sheet, current_row, 8, campaign['Days with Negative Score'], positive_data_format)
                safe_write(negative_score_sheet, current_row, 9, campaign['Negative Score Dates'], positive_data_format)
                safe_write(negative_score_sheet, current_row, 10, campaign['Reason'], positive_data_format)
                current_row += 1
        else:
            safe_write(negative_score_sheet, current_row, 0, "No campaigns found with positive score (0 and above)", positive_data_format)
            current_row += 1

        # Add summary for positive campaigns
        current_row += 2
        safe_write(negative_score_sheet, current_row, 0, "SUMMARY - POSITIVE CAMPAIGNS", positive_header_format)
        safe_write(negative_score_sheet, current_row + 1, 0, f"Campaigns with positive score (0 and above): {len(positive_campaigns)}", positive_data_format)
        
        # ==== PART 4: LAST DATE NEGATIVE SCORE CAMPAIGNS (ONLY VALID PRODUCTS) ====
        current_row += 5
        
        safe_write(negative_score_sheet, current_row, 0, "CAMPAIGNS WITH NEGATIVE SCORE ON LAST DATE", last_date_header_format)
        current_row += 1
        
        # Headers for last date table
        last_date_headers = ["Product", "Campaign Name", "C.P.P", "B.E", "Amount Spent (USD)", "Score", "Last Date", "Last Date Score", 
                           "Last Date Amount Spent (USD)", "Last Date Purchases", "Reason"]

        for col_num, header in enumerate(last_date_headers):
            safe_write(negative_score_sheet, current_row, col_num, header, last_date_header_format)
        current_row += 1

        # Get the last date
        last_date = unique_dates[-1] if unique_dates else None
        
        # Create set of campaigns already in first three tables
        already_processed_campaigns = set()
        for campaign in all_negative_campaigns:
            already_processed_campaigns.add((campaign['Product'], campaign['Campaign Name']))

        # Analyze campaigns for last date negative scores (ONLY VALID PRODUCTS)
        last_date_negative_campaigns = []
        
        if last_date:
            for product, product_df in valid_products:
                for campaign_name, campaign_group in product_df.groupby("Campaign Name"):
                    if (str(product), str(campaign_name)) in already_processed_campaigns:
                        continue
                    
                    last_date_data = campaign_group[campaign_group['Date'].astype(str) == last_date]
                    if last_date_data.empty:
                        continue
                    
                    row_data = last_date_data.iloc[0]
                    
                    # Calculate CPP and Amount Spent
                    total_amount_spent_usd = campaign_group.get("Amount Spent (USD)", 0).sum() if "Amount Spent (USD)" in campaign_group.columns else 0
                    total_purchases = campaign_group.get("Purchases", 0).sum() if "Purchases" in campaign_group.columns else 0
                    
                    cpp = 0
                    if total_amount_spent_usd > 0 and total_purchases == 0:
                        cpp = total_amount_spent_usd / 1
                    elif total_purchases > 0:
                        cpp = total_amount_spent_usd / total_purchases
                    
                    # Calculate B.E
                    product_costs = []
                    for date in unique_dates:
                        date_cost = product_date_cost_inputs.get(product, {}).get(date, 0)
                        if date_cost > 0:
                            product_costs.append(date_cost)
                    avg_product_cost = sum(product_costs) / len(product_costs) if product_costs else 0
                    
                    product_avg_price = product_total_avg_prices.get(product, 0)
                    product_delivery_rate = product_total_delivery_rates.get(product, 0)
                    
                    be = 0
                    if product_avg_price > 0 and (total_purchases > 0 or (total_purchases == 0 and total_amount_spent_usd > 0)):
                        calc_purchases = 1 if (total_purchases == 0 and total_amount_spent_usd > 0) else total_purchases
                        delivery_rate = product_delivery_rate / 100 if product_delivery_rate > 1 else product_delivery_rate
                        revenue = product_avg_price * calc_purchases * delivery_rate
                        costs = (77 * calc_purchases) + (65 * calc_purchases) + (avg_product_cost * calc_purchases * delivery_rate)
                        if calc_purchases > 0:
                            be = (revenue - costs) / 100 / calc_purchases
                    
                    # Calculate overall Score
                    overall_score = 0
                    if product_avg_price > 0 and (total_purchases > 0 or (total_purchases == 0 and total_amount_spent_usd > 0)):
                        calc_purchases = 1 if (total_purchases == 0 and total_amount_spent_usd > 0) else total_purchases
                        delivery_rate = product_delivery_rate / 100 if product_delivery_rate > 1 else product_delivery_rate
                        revenue = product_avg_price * calc_purchases * delivery_rate
                        costs = (total_amount_spent_usd * 100) + (77 * calc_purchases) + (65 * calc_purchases) + (avg_product_cost * calc_purchases * delivery_rate)
                        profit = revenue - costs
                        overall_score = (profit / (revenue * 0.1)) if revenue > 0 else 0
                    
                    # Get last date data
                    amount_spent = row_data.get("Amount Spent (USD)", 0) or 0
                    purchases = row_data.get("Purchases", 0) or 0
                    
                    date_avg_price = product_date_avg_prices.get(product, {}).get(last_date, 0)
                    date_delivery_rate = product_date_delivery_rates.get(product, {}).get(last_date, 0)
                    date_product_cost = product_date_cost_inputs.get(product, {}).get(last_date, 0)
                    
                    # Calculate last date score
                    if date_avg_price > 0 and (purchases > 0 or (purchases == 0 and amount_spent > 0)):
                        calc_purchases = 1 if (purchases == 0 and amount_spent > 0) else purchases
                        delivery_rate = date_delivery_rate / 100 if date_delivery_rate > 1 else date_delivery_rate
                        revenue = date_avg_price * calc_purchases * delivery_rate
                        costs = (amount_spent * 100) + (77 * calc_purchases) + (65 * calc_purchases) + (date_product_cost * calc_purchases * delivery_rate)
                        profit = revenue - costs
                        score = (profit / (revenue * 0.1)) if revenue > 0 else 0
                        
                        if score < 0:
                            last_date_campaign = {
                                'Product': str(product),
                                'Campaign Name': str(campaign_name),
                                'C.P.P': round(cpp, 2),
                                'B.E': round(be, 2),
                                'Amount Spent (USD)': round(total_amount_spent_usd, 2),
                                'Score': round(overall_score, 2),
                                'Last Date': format_date_readable(last_date),
                                'Last Date Score': round(score, 2),
                                'Last Date Amount Spent (USD)': round(amount_spent, 2),
                                'Last Date Purchases': int(purchases),
                                'Reason': f"Negative score ({round(score, 2)}) on last date ({format_date_readable(last_date)})"
                            }
                            
                            last_date_negative_campaigns.append(last_date_campaign)

        # Write last date negative campaigns
        if last_date_negative_campaigns:
            last_date_negative_campaigns.sort(key=lambda x: x['Last Date Score'])
            
            for campaign in last_date_negative_campaigns:
                safe_write(negative_score_sheet, current_row, 0, campaign['Product'], last_date_data_format)
                safe_write(negative_score_sheet, current_row, 1, campaign['Campaign Name'], last_date_data_format)
                safe_write(negative_score_sheet, current_row, 2, campaign['C.P.P'], last_date_data_format)
                safe_write(negative_score_sheet, current_row, 3, campaign['B.E'], last_date_data_format)
                safe_write(negative_score_sheet, current_row, 4, campaign['Amount Spent (USD)'], last_date_data_format)
                safe_write(negative_score_sheet, current_row, 5, campaign['Score'], last_date_data_format)
                safe_write(negative_score_sheet, current_row, 6, campaign['Last Date'], last_date_data_format)
                safe_write(negative_score_sheet, current_row, 7, campaign['Last Date Score'], last_date_data_format)
                safe_write(negative_score_sheet, current_row, 8, campaign['Last Date Amount Spent (USD)'], last_date_data_format)
                safe_write(negative_score_sheet, current_row, 9, campaign['Last Date Purchases'], last_date_data_format)
                safe_write(negative_score_sheet, current_row, 10, campaign['Reason'], last_date_data_format)
                current_row += 1
        else:
            safe_write(negative_score_sheet, current_row, 0, f"No campaigns found with negative score on last date ({format_date_readable(last_date) if last_date else 'N/A'})", last_date_data_format)
            current_row += 1

        # Add summary for last date table
        current_row += 2
        safe_write(negative_score_sheet, current_row, 0, "SUMMARY - LAST DATE TABLE", last_date_header_format)
        safe_write(negative_score_sheet, current_row + 1, 0, f"Last date analyzed: {format_date_readable(last_date) if last_date else 'N/A'}", last_date_data_format)
        safe_write(negative_score_sheet, current_row + 2, 0, f"Campaigns with negative score on last date: {len(last_date_negative_campaigns)}", last_date_data_format)
        safe_write(negative_score_sheet, current_row + 3, 0, f"Campaigns excluded (already in other tables): {len(already_processed_campaigns)}", last_date_data_format)
        
        # Add overall summary
        current_row += 5
        safe_write(negative_score_sheet, current_row, 0, "OVERALL SUMMARY", severe_negative_header_format)
        
        # Count total campaigns (ONLY VALID PRODUCTS)
        total_campaigns = 0
        for product, product_df in valid_products:
            total_campaigns += len(product_df.groupby("Campaign Name"))

        safe_write(negative_score_sheet, current_row + 1, 0, f"Total campaigns analyzed (valid products only): {total_campaigns}", severe_negative_data_format)
        safe_write(negative_score_sheet, current_row + 2, 0, f"Total campaigns with negative score: {len(all_negative_campaigns)}", severe_negative_data_format)
        safe_write(negative_score_sheet, current_row + 3, 0, f"Severe negative campaigns (-10 to -1): {len(severe_negative_campaigns)}", severe_negative_data_format)
        safe_write(negative_score_sheet, current_row + 4, 0, f"Moderate negative campaigns (-1 to 0, excluding 0): {len(moderate_negative_campaigns)}", moderate_negative_data_format)
        safe_write(negative_score_sheet, current_row + 5, 0, f"Positive campaigns (0 and above): {len(positive_campaigns)}", positive_data_format)
        safe_write(negative_score_sheet, current_row + 6, 0, f"Last date negative campaigns: {len(last_date_negative_campaigns)}", last_date_data_format)
        safe_write(negative_score_sheet, current_row + 7, 0, f"Days threshold used: {selected_days} out of {len(unique_dates)} total unique dates", severe_negative_data_format)
        safe_write(negative_score_sheet, current_row + 8, 0, f"Date range analyzed: {min(unique_dates)} to {max(unique_dates)}" if unique_dates else "No dates found", severe_negative_data_format)
        safe_write(negative_score_sheet, current_row + 9, 0, f"Products excluded from analysis: {len(excluded_products)} (zero cost input and zero delivery rate)", severe_negative_data_format)

        # Set column widths for negative score sheet
        negative_score_sheet.set_column(0, 0, 20)  # Product
        negative_score_sheet.set_column(1, 1, 35)  # Campaign Name
        negative_score_sheet.set_column(2, 2, 15)  # C.P.P
        negative_score_sheet.set_column(3, 3, 15)  # B.E
        negative_score_sheet.set_column(4, 4, 20)  # Amount Spent (USD)
        negative_score_sheet.set_column(5, 5, 15)  # Score
        negative_score_sheet.set_column(6, 6, 15)  # Total Dates / Last Date
        negative_score_sheet.set_column(7, 7, 15)  # Days Checked / Last Date Score
        negative_score_sheet.set_column(8, 8, 25)  # Days with Negative Score / Last Date Amount Spent
        negative_score_sheet.set_column(9, 9, 40)  # Negative Score Dates / Last Date Purchases
        negative_score_sheet.set_column(10, 10, 40)  # Comment / Reason
        negative_score_sheet.set_column(11, 11, 40)  # Reason (for severe campaigns only)
        negative_score_sheet.set_column(12, 12, 40)
        
        # ==== NEW SHEET: Scalable Campaigns (Based on Score) ====
        scalable_sheet = workbook.add_worksheet("Scalable Campaigns")
        
        # Formats for scalable campaigns sheet
        scalable_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#4CAF50", "font_name": "Calibri", "font_size": 11
        })
        moderate_scalable_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#8BC34A", "font_name": "Calibri", "font_size": 11
        })
        high_scalable_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#2E7D32", "font_name": "Calibri", "font_size": 11
        })
        moderate_scalable_data_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#F1F8E9", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        high_scalable_data_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#C8E6C9", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        
        current_row = 0
        
        # Build a lookup of Score values using DAY-BY-DAY calculation (matching Negative Score Campaigns)
        campaign_score_lookup = {}
        
        for product, product_df in valid_products:
            for campaign_name, campaign_group in product_df.groupby("Campaign Name"):
                # Calculate campaign totals
                total_amount_spent_usd = campaign_group.get("Amount Spent (USD)", 0).sum() if "Amount Spent (USD)" in campaign_group.columns else 0
                total_purchases = campaign_group.get("Purchases", 0).sum() if "Purchases" in campaign_group.columns else 0
                
                # Get product-level values
                product_avg_price = round(product_total_avg_prices.get(product, 0), 2)
                product_delivery_rate = round(product_total_delivery_rates.get(product, 0), 2)
                
                # Calculate overall Score using day-by-day method (matching Negative Score Campaigns)
                overall_score = 0
                total_net_profit_sum = 0
                campaign_dates = sorted([str(d) for d in campaign_group['Date'].unique() if pd.notna(d) and str(d).strip() != ''])
                
                if product_avg_price > 0:
                    # Calculate net profit by summing day-by-day
                    for date in campaign_dates:
                        date_data = campaign_group[campaign_group['Date'].astype(str) == date]
                        if not date_data.empty:
                            row_data = date_data.iloc[0]
                            
                            date_amount_spent = round(row_data.get("Amount Spent (USD)", 0) if pd.notna(row_data.get("Amount Spent (USD)")) else 0, 2)
                            date_purchases = round(row_data.get("Purchases", 0) if pd.notna(row_data.get("Purchases")) else 0, 2)
                            
                            date_avg_price = round(product_date_avg_prices.get(product, {}).get(date, 0), 2)
                            date_delivery_rate = round(product_date_delivery_rates.get(product, {}).get(date, 0), 2)
                            date_product_cost = round(product_date_cost_inputs.get(product, {}).get(date, 0), 2)
                            
                            calc_purchases_date = round(date_purchases, 2)
                            delivery_rate_date = round(date_delivery_rate / 100 if date_delivery_rate > 1 else date_delivery_rate, 2)
                            
                            delivered_orders = round(calc_purchases_date * delivery_rate_date, 2)
                            net_revenue = round(delivered_orders * date_avg_price, 2)
                            total_product_cost_date = round(delivered_orders * date_product_cost, 2)
                            total_shipping_cost_date = round(calc_purchases_date * 77, 2)
                            total_operational_cost_date = round(calc_purchases_date * 65, 2)
                            
                            date_net_profit = round(net_revenue - (date_amount_spent * 100) - total_shipping_cost_date - total_operational_cost_date - total_product_cost_date, 2)
                            total_net_profit_sum += round(date_net_profit, 2)
                    
                    # Calculate Net Profit %
                    calc_purchases_total = 1 if (total_purchases == 0 and total_amount_spent_usd > 0) else total_purchases
                    delivery_rate_total = round(product_delivery_rate / 100 if product_delivery_rate > 1 else product_delivery_rate, 2)
                    
                    numerator_total = round(total_net_profit_sum, 2)
                    denominator_total = round(product_avg_price * calc_purchases_total * delivery_rate_total, 2)
                    net_profit_percentage = round((numerator_total / denominator_total * 100), 2) if denominator_total > 0 else 0
                    
                    # Score = Net Profit % / 10
                    overall_score = round(net_profit_percentage / 10, 2)
                
                # Calculate CPP and BE
                cpp = 0
                if total_amount_spent_usd > 0 and total_purchases == 0:
                    cpp = total_amount_spent_usd / 1
                elif total_purchases > 0:
                    cpp = total_amount_spent_usd / total_purchases
                
                product_costs = []
                for date in unique_dates:
                    date_cost = product_date_cost_inputs.get(product, {}).get(date, 0)
                    if date_cost > 0:
                        product_costs.append(date_cost)
                avg_product_cost = sum(product_costs) / len(product_costs) if product_costs else 0
                
                be = 0
                if product_avg_price > 0 and (total_purchases > 0 or (total_purchases == 0 and total_amount_spent_usd > 0)):
                    calc_purchases = 1 if (total_purchases == 0 and total_amount_spent_usd > 0) else total_purchases
                    delivery_rate = product_delivery_rate / 100 if product_delivery_rate > 1 else product_delivery_rate
                    revenue = product_avg_price * calc_purchases * delivery_rate
                    costs = (77 * calc_purchases) + (65 * calc_purchases) + (avg_product_cost * calc_purchases * delivery_rate)
                    if calc_purchases > 0:
                        be = (revenue - costs) / 100 / calc_purchases
                
                # Store in lookup
                campaign_key = (str(product), str(campaign_name))
                campaign_score_lookup[campaign_key] = {
                    'score': overall_score,
                    'total_amount_spent': round(total_amount_spent_usd, 2),
                    'total_purchases': int(total_purchases),
                    'cpp': round(cpp, 2),
                    'be': round(be, 2),
                    'total_dates': len([d for d in campaign_group['Date'].unique() if pd.notna(d)])
                }
        
        # Collect scalable campaigns (Score > 0)
        scalable_campaigns = []
        
        for campaign_key, campaign_data in campaign_score_lookup.items():
            if campaign_data['score'] > 0:
                scalable_campaign = {
                    'Product': campaign_key[0],
                    'Campaign Name': campaign_key[1],
                    'C.P.P': campaign_data['cpp'],
                    'B.E': campaign_data['be'],
                    'Total Amount Spent (USD)': campaign_data['total_amount_spent'],
                    'Total Purchases': campaign_data['total_purchases'],
                    'Score': campaign_data['score'],
                    'Total Dates': campaign_data['total_dates']
                }
                scalable_campaigns.append(scalable_campaign)
        
        # Split into FOUR groups based on Score AND Amount Spent
        moderate_scalable_high_spend = [c for c in scalable_campaigns if 1 < c['Score'] <= 2 and c['Total Amount Spent (USD)'] >= 10]
        moderate_scalable_low_spend = [c for c in scalable_campaigns if 1 < c['Score'] <= 2 and c['Total Amount Spent (USD)'] < 10]
        high_scalable_high_spend = [c for c in scalable_campaigns if c['Score'] > 2 and c['Total Amount Spent (USD)'] >= 10]
        high_scalable_low_spend = [c for c in scalable_campaigns if c['Score'] > 2 and c['Total Amount Spent (USD)'] < 10]
        
        # Sort all four groups by Score (highest first)
        moderate_scalable_high_spend.sort(key=lambda x: x['Score'], reverse=True)
        moderate_scalable_low_spend.sort(key=lambda x: x['Score'], reverse=True)
        high_scalable_high_spend.sort(key=lambda x: x['Score'], reverse=True)
        high_scalable_low_spend.sort(key=lambda x: x['Score'], reverse=True)
        
        # ==== TABLE 1A: MODERATE SCALABLE - HIGH SPEND (Amount >= $10) ====
        safe_write(scalable_sheet, current_row, 0, "MODERATE SCALABLE CAMPAIGNS (1 < SCORE ≤ 2) - AMOUNT SPENT ≥ $10", moderate_scalable_header_format)
        current_row += 1
        
        # Headers
        scalable_headers = ["Product", "Campaign Name", "C.P.P", "B.E", "Total Amount Spent (USD)", 
                           "Total Purchases", "Score", "Total Dates"]
        
        for col_num, header in enumerate(scalable_headers):
            safe_write(scalable_sheet, current_row, col_num, header, moderate_scalable_header_format)
        current_row += 1
        
        # Write moderate scalable campaigns - HIGH SPEND
        if moderate_scalable_high_spend:
            for campaign in moderate_scalable_high_spend:
                safe_write(scalable_sheet, current_row, 0, campaign['Product'], moderate_scalable_data_format)
                safe_write(scalable_sheet, current_row, 1, campaign['Campaign Name'], moderate_scalable_data_format)
                safe_write(scalable_sheet, current_row, 2, campaign['C.P.P'], moderate_scalable_data_format)
                safe_write(scalable_sheet, current_row, 3, campaign['B.E'], moderate_scalable_data_format)
                safe_write(scalable_sheet, current_row, 4, campaign['Total Amount Spent (USD)'], moderate_scalable_data_format)
                safe_write(scalable_sheet, current_row, 5, campaign['Total Purchases'], moderate_scalable_data_format)
                safe_write(scalable_sheet, current_row, 6, campaign['Score'], moderate_scalable_data_format)
                safe_write(scalable_sheet, current_row, 7, campaign['Total Dates'], moderate_scalable_data_format)
                current_row += 1
        else:
            safe_write(scalable_sheet, current_row, 0, "No campaigns found with Score between 0 and 2 and Amount Spent >= $10", moderate_scalable_data_format)
            current_row += 1
        
        # Summary for moderate scalable - HIGH SPEND
        current_row += 2
        safe_write(scalable_sheet, current_row, 0, "SUMMARY - MODERATE SCALABLE (HIGH SPEND ≥ $10)", moderate_scalable_header_format)
        current_row += 1
        
        total_moderate_high_campaigns = len(moderate_scalable_high_spend)
        total_moderate_high_spend = sum([c['Total Amount Spent (USD)'] for c in moderate_scalable_high_spend])
        total_moderate_high_purchases = sum([c['Total Purchases'] for c in moderate_scalable_high_spend])
        avg_moderate_high_score = sum([c['Score'] for c in moderate_scalable_high_spend]) / total_moderate_high_campaigns if total_moderate_high_campaigns > 0 else 0
        
        safe_write(scalable_sheet, current_row, 0, f"Total Campaigns: {total_moderate_high_campaigns}", moderate_scalable_data_format)
        safe_write(scalable_sheet, current_row + 1, 0, f"Total Amount Spent (USD): ${total_moderate_high_spend:,.2f}", moderate_scalable_data_format)
        safe_write(scalable_sheet, current_row + 2, 0, f"Total Purchases: {total_moderate_high_purchases:,}", moderate_scalable_data_format)
        safe_write(scalable_sheet, current_row + 3, 0, f"Average Score: {round(avg_moderate_high_score, 2)}", moderate_scalable_data_format)
        
        current_row += 7
        
        # ==== TABLE 1B: MODERATE SCALABLE - LOW SPEND (Amount < $10) ====
        safe_write(scalable_sheet, current_row, 0, "MODERATE SCALABLE CAMPAIGNS (1 < SCORE ≤ 2) - AMOUNT SPENT < $10", moderate_scalable_header_format)
        current_row += 1
        
        for col_num, header in enumerate(scalable_headers):
            safe_write(scalable_sheet, current_row, col_num, header, moderate_scalable_header_format)
        current_row += 1
        
        # Write moderate scalable campaigns - LOW SPEND
        if moderate_scalable_low_spend:
            for campaign in moderate_scalable_low_spend:
                safe_write(scalable_sheet, current_row, 0, campaign['Product'], moderate_scalable_data_format)
                safe_write(scalable_sheet, current_row, 1, campaign['Campaign Name'], moderate_scalable_data_format)
                safe_write(scalable_sheet, current_row, 2, campaign['C.P.P'], moderate_scalable_data_format)
                safe_write(scalable_sheet, current_row, 3, campaign['B.E'], moderate_scalable_data_format)
                safe_write(scalable_sheet, current_row, 4, campaign['Total Amount Spent (USD)'], moderate_scalable_data_format)
                safe_write(scalable_sheet, current_row, 5, campaign['Total Purchases'], moderate_scalable_data_format)
                safe_write(scalable_sheet, current_row, 6, campaign['Score'], moderate_scalable_data_format)
                safe_write(scalable_sheet, current_row, 7, campaign['Total Dates'], moderate_scalable_data_format)
                current_row += 1
        else:
            safe_write(scalable_sheet, current_row, 0, "No campaigns found with Score between 0 and 2 and Amount Spent < $10", moderate_scalable_data_format)
            current_row += 1
        
        # Summary for moderate scalable - LOW SPEND
        current_row += 2
        safe_write(scalable_sheet, current_row, 0, "SUMMARY - MODERATE SCALABLE (LOW SPEND < $10)", moderate_scalable_header_format)
        current_row += 1
        
        total_moderate_low_campaigns = len(moderate_scalable_low_spend)
        total_moderate_low_spend = sum([c['Total Amount Spent (USD)'] for c in moderate_scalable_low_spend])
        total_moderate_low_purchases = sum([c['Total Purchases'] for c in moderate_scalable_low_spend])
        avg_moderate_low_score = sum([c['Score'] for c in moderate_scalable_low_spend]) / total_moderate_low_campaigns if total_moderate_low_campaigns > 0 else 0
        
        safe_write(scalable_sheet, current_row, 0, f"Total Campaigns: {total_moderate_low_campaigns}", moderate_scalable_data_format)
        safe_write(scalable_sheet, current_row + 1, 0, f"Total Amount Spent (USD): ${total_moderate_low_spend:,.2f}", moderate_scalable_data_format)
        safe_write(scalable_sheet, current_row + 2, 0, f"Total Purchases: {total_moderate_low_purchases:,}", moderate_scalable_data_format)
        safe_write(scalable_sheet, current_row + 3, 0, f"Average Score: {round(avg_moderate_low_score, 2)}", moderate_scalable_data_format)
        
        current_row += 7
        
        # ==== TABLE 2A: HIGH SCALABLE - HIGH SPEND (Amount >= $10) ====
        safe_write(scalable_sheet, current_row, 0, "HIGH SCALABLE CAMPAIGNS (SCORE > 2) - AMOUNT SPENT ≥ $10", high_scalable_header_format)
        current_row += 1
        
        for col_num, header in enumerate(scalable_headers):
            safe_write(scalable_sheet, current_row, col_num, header, high_scalable_header_format)
        current_row += 1
        
        # Write high scalable campaigns - HIGH SPEND
        if high_scalable_high_spend:
            for campaign in high_scalable_high_spend:
                safe_write(scalable_sheet, current_row, 0, campaign['Product'], high_scalable_data_format)
                safe_write(scalable_sheet, current_row, 1, campaign['Campaign Name'], high_scalable_data_format)
                safe_write(scalable_sheet, current_row, 2, campaign['C.P.P'], high_scalable_data_format)
                safe_write(scalable_sheet, current_row, 3, campaign['B.E'], high_scalable_data_format)
                safe_write(scalable_sheet, current_row, 4, campaign['Total Amount Spent (USD)'], high_scalable_data_format)
                safe_write(scalable_sheet, current_row, 5, campaign['Total Purchases'], high_scalable_data_format)
                safe_write(scalable_sheet, current_row, 6, campaign['Score'], high_scalable_data_format)
                safe_write(scalable_sheet, current_row, 7, campaign['Total Dates'], high_scalable_data_format)
                current_row += 1
        else:
            safe_write(scalable_sheet, current_row, 0, "No campaigns found with Score > 2 and Amount Spent >= $10", high_scalable_data_format)
            current_row += 1
        
        # Summary for high scalable - HIGH SPEND
        current_row += 2
        safe_write(scalable_sheet, current_row, 0, "SUMMARY - HIGH SCALABLE (HIGH SPEND ≥ $10)", high_scalable_header_format)
        current_row += 1
        
        total_high_high_campaigns = len(high_scalable_high_spend)
        total_high_high_spend = sum([c['Total Amount Spent (USD)'] for c in high_scalable_high_spend])
        total_high_high_purchases = sum([c['Total Purchases'] for c in high_scalable_high_spend])
        avg_high_high_score = sum([c['Score'] for c in high_scalable_high_spend]) / total_high_high_campaigns if total_high_high_campaigns > 0 else 0
        
        safe_write(scalable_sheet, current_row, 0, f"Total Campaigns: {total_high_high_campaigns}", high_scalable_data_format)
        safe_write(scalable_sheet, current_row + 1, 0, f"Total Amount Spent (USD): ${total_high_high_spend:,.2f}", high_scalable_data_format)
        safe_write(scalable_sheet, current_row + 2, 0, f"Total Purchases: {total_high_high_purchases:,}", high_scalable_data_format)
        safe_write(scalable_sheet, current_row + 3, 0, f"Average Score: {round(avg_high_high_score, 2)}", high_scalable_data_format)
        
        current_row += 7
        
        # ==== TABLE 2B: HIGH SCALABLE - LOW SPEND (Amount < $10) ====
        safe_write(scalable_sheet, current_row, 0, "HIGH SCALABLE CAMPAIGNS (SCORE > 2) - AMOUNT SPENT < $10", high_scalable_header_format)
        current_row += 1
        
        for col_num, header in enumerate(scalable_headers):
            safe_write(scalable_sheet, current_row, col_num, header, high_scalable_header_format)
        current_row += 1
        
        # Write high scalable campaigns - LOW SPEND
        if high_scalable_low_spend:
            for campaign in high_scalable_low_spend:
                safe_write(scalable_sheet, current_row, 0, campaign['Product'], high_scalable_data_format)
                safe_write(scalable_sheet, current_row, 1, campaign['Campaign Name'], high_scalable_data_format)
                safe_write(scalable_sheet, current_row, 2, campaign['C.P.P'], high_scalable_data_format)
                safe_write(scalable_sheet, current_row, 3, campaign['B.E'], high_scalable_data_format)
                safe_write(scalable_sheet, current_row, 4, campaign['Total Amount Spent (USD)'], high_scalable_data_format)
                safe_write(scalable_sheet, current_row, 5, campaign['Total Purchases'], high_scalable_data_format)
                safe_write(scalable_sheet, current_row, 6, campaign['Score'], high_scalable_data_format)
                safe_write(scalable_sheet, current_row, 7, campaign['Total Dates'], high_scalable_data_format)
                current_row += 1
        else:
            safe_write(scalable_sheet, current_row, 0, "No campaigns found with Score > 2 and Amount Spent < $10", high_scalable_data_format)
            current_row += 1
        
        # Summary for high scalable - LOW SPEND
        current_row += 2
        safe_write(scalable_sheet, current_row, 0, "SUMMARY - HIGH SCALABLE (LOW SPEND < $10)", high_scalable_header_format)
        current_row += 1
        
        total_high_low_campaigns = len(high_scalable_low_spend)
        total_high_low_spend = sum([c['Total Amount Spent (USD)'] for c in high_scalable_low_spend])
        total_high_low_purchases = sum([c['Total Purchases'] for c in high_scalable_low_spend])
        avg_high_low_score = sum([c['Score'] for c in high_scalable_low_spend]) / total_high_low_campaigns if total_high_low_campaigns > 0 else 0
        
        safe_write(scalable_sheet, current_row, 0, f"Total Campaigns: {total_high_low_campaigns}", high_scalable_data_format)
        safe_write(scalable_sheet, current_row + 1, 0, f"Total Amount Spent (USD): ${total_high_low_spend:,.2f}", high_scalable_data_format)
        safe_write(scalable_sheet, current_row + 2, 0, f"Total Purchases: {total_high_low_purchases:,}", high_scalable_data_format)
        safe_write(scalable_sheet, current_row + 3, 0, f"Average Score: {round(avg_high_low_score, 2)}", high_scalable_data_format)
        
        # Overall summary
        current_row += 7
        safe_write(scalable_sheet, current_row, 0, "OVERALL SUMMARY - ALL SCALABLE CAMPAIGNS", scalable_header_format)
        current_row += 1
        
        total_scalable = len(scalable_campaigns)
        total_scalable_spend = sum([c['Total Amount Spent (USD)'] for c in scalable_campaigns])
        total_scalable_purchases = sum([c['Total Purchases'] for c in scalable_campaigns])
        
        total_moderate_campaigns = total_moderate_high_campaigns + total_moderate_low_campaigns
        total_high_campaigns = total_high_high_campaigns + total_high_low_campaigns
        
        safe_write(scalable_sheet, current_row, 0, f"Total Scalable Campaigns (Score > 0): {total_scalable}", moderate_scalable_data_format)
        safe_write(scalable_sheet, current_row + 1, 0, f"  • Moderate (1 < Score ≤ 2): {total_moderate_campaigns}", moderate_scalable_data_format)
        safe_write(scalable_sheet, current_row + 2, 0, f"    - High Spend (≥ $10): {total_moderate_high_campaigns}", moderate_scalable_data_format)
        safe_write(scalable_sheet, current_row + 3, 0, f"    - Low Spend (< $10): {total_moderate_low_campaigns}", moderate_scalable_data_format)
        safe_write(scalable_sheet, current_row + 4, 0, f"  • High (Score > 2): {total_high_campaigns}", high_scalable_data_format)
        safe_write(scalable_sheet, current_row + 5, 0, f"    - High Spend (≥ $10): {total_high_high_campaigns}", high_scalable_data_format)
        safe_write(scalable_sheet, current_row + 6, 0, f"    - Low Spend (< $10): {total_high_low_campaigns}", high_scalable_data_format)
        safe_write(scalable_sheet, current_row + 7, 0, f"Total Amount Spent (USD): ${total_scalable_spend:,.2f}", moderate_scalable_data_format)
        safe_write(scalable_sheet, current_row + 8, 0, f"Total Purchases: {total_scalable_purchases:,}", moderate_scalable_data_format)
        
        # Set column widths for scalable campaigns sheet
        scalable_sheet.set_column(0, 0, 25)  # Product
        scalable_sheet.set_column(1, 1, 40)  # Campaign Name
        scalable_sheet.set_column(2, 2, 15)  # C.P.P
        scalable_sheet.set_column(3, 3, 15)  # B.E
        scalable_sheet.set_column(4, 4, 25)  # Total Amount Spent (USD)
        scalable_sheet.set_column(5, 5, 18)  # Total Purchases
        scalable_sheet.set_column(6, 6, 18)  # Score
        scalable_sheet.set_column(7, 7, 15)  # Total Dates
        
        
        
    return output.getvalue()




st.header("📥 Download Processed Files")

# ---- SHOPIFY DOWNLOAD ----
if df_shopify is not None:
    export_df = df_shopify.drop(columns=["Product Name", "Canonical Product"], errors="ignore")

    # Use simple structure for staff version
    shopify_excel = convert_shopify_to_excel_staff_with_date_columns_corrected(export_df)
    st.download_button(
        label="📥 Download Staff Shopify File",
        data=shopify_excel,
        file_name="staff_shopify_processed.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
else:
    st.warning("⚠️ Please upload Shopify files to process.")

# ---- CAMPAIGN DOWNLOAD ----
if campaign_files:
    def convert_df_to_excel(df):
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Processed Data")
        return output.getvalue()

    
    # Download final campaign data (structured format for staff)
    if 'df_final_campaign' in locals() and not df_final_campaign.empty:
        final_campaign_excel = convert_final_campaign_to_excel_staff_with_date_columns_fixed(df_final_campaign)
        if final_campaign_excel:
            st.download_button(
                label="🎯 Download Staff Campaign File",
                data=final_campaign_excel,
                file_name="staff_final_campaign_data.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

# ---- SUMMARY SECTION ----
if campaign_files or shopify_files or old_merged_files:
    st.header("📊 Processing Summary")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Campaign Files Uploaded", len(campaign_files) if campaign_files else 0)
        if df_campaign is not None:
            st.metric("Total Campaigns", len(df_campaign))
    
    with col2:
        st.metric("Shopify Files Uploaded", len(shopify_files) if shopify_files else 0)
        if df_shopify is not None:
            st.metric("Total Product Variants", len(df_shopify))
    
    with col3:
        st.metric("Reference Files Uploaded", len(old_merged_files) if old_merged_files else 0)
        if df_old_merged is not None:
            st.metric("Reference Records", len(df_old_merged))

    # Show date information
    if df_shopify is not None and 'Date' in df_shopify.columns:
        unique_dates = df_shopify['Date'].unique()
        unique_dates = [str(d) for d in unique_dates if pd.notna(d) and str(d).strip() != '']
        st.info(f"📅 Found {len(unique_dates)} unique dates: {', '.join(sorted(unique_dates)[:5])}{'...' if len(unique_dates) > 5 else ''}")
        
        
        
        
    
