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
        
        agg_dict = {"Amount spent (USD)": "sum"}
        if has_purchases:
            agg_dict["Purchases"] = "sum"
        
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
            
            # Now allocate ad spend to Shopify variants based on their share of items sold per product per date
            for product, product_df in df_shopify.groupby("Canonical Product"):
                if product in campaign_spend_by_product_date:
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
            
            # For products without date-specific campaign data, fall back to total allocation
            ad_spend_map = dict(zip(grouped_campaign["Product"], grouped_campaign["Total Amount Spent (USD)"]))
            
            for product, product_df in df_shopify.groupby("Canonical Product"):
                # Only allocate total spend for variants that don't have date-specific allocation
                variants_without_date_allocation = product_df[product_df["Ad Spend (USD)"] == 0]
                
                if not variants_without_date_allocation.empty and product in ad_spend_map:
                    total_items = variants_without_date_allocation["Net items sold"].sum()
                    if total_items > 0:
                        total_spend_usd = ad_spend_map[product]
                        
                        # Allocate spend based on items sold
                        ratio = variants_without_date_allocation["Net items sold"] / total_items
                        df_shopify.loc[variants_without_date_allocation.index, "Ad Spend (USD)"] = total_spend_usd * ratio

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

def convert_shopify_to_excel_staff_simple(df):
    """Simple Shopify Excel conversion for staff without dates"""
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet("Shopify Staff")
        writer.sheets["Shopify Staff"] = worksheet

        # Formats
        header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#BDD7EE", "font_name": "Calibri", "font_size": 11
        })
        grand_total_format = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#EEEE0E", "font_name": "Calibri", "font_size": 11
        })
        product_total_format_low = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#DC4E23", "font_name": "Calibri", "font_size": 11
        })
        variant_format_low = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#FFCCCB", "font_name": "Calibri", "font_size": 11
        })
        product_total_format_high = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#E7EE94", "font_name": "Calibri", "font_size": 11
        })
        variant_format_high = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "font_name": "Calibri", "font_size": 11
        })

        # Visible columns for staff
        visible_cols = [
            "Product title", "Product variant title", "Product variant price",
            "Net items sold", "Ad Spend (USD)",
            "Product Cost (Input)", "Delivery Rate", "Score"
        ]

        # Write headers
        for col_num, col_name in enumerate(visible_cols):
            worksheet.write(0, col_num, col_name, header_format)

        # Grand total row
        grand_total_row = 1
        worksheet.write(grand_total_row, 0, "GRAND TOTAL", grand_total_format)
        worksheet.write(grand_total_row, 1, "ALL PRODUCTS", grand_total_format)

        row = grand_total_row + 1
        product_total_rows = []

        for product, product_df in df.groupby("Product title"):
            product_total_row_idx = row
            product_total_rows.append(product_total_row_idx)

            # Calculate total net items sold for this product to determine formatting
            total_net_items = product_df["Net items sold"].fillna(0).sum()
            is_low_performing = total_net_items < 5

            # Choose format based on performance
            current_product_format = product_total_format_low if is_low_performing else product_total_format_high
            current_variant_format = variant_format_low if is_low_performing else variant_format_high

            # Product total label
            worksheet.write(product_total_row_idx, 0, product, current_product_format)
            worksheet.write(product_total_row_idx, 1, "ALL VARIANTS (TOTAL)", current_product_format)

            # Variants
            row += 1
            first_variant_row = row
            for _, variant in product_df.iterrows():
                variant_row_idx = row
                excel_row = variant_row_idx + 1  # Excel is 1-based

                # Get values
                P = variant.get("Product variant price", 0) or 0
                S = variant.get("Net items sold", 0) or 0
                A_usd = variant.get("Ad Spend (USD)", 0.0) or 0.0
                C = variant.get("Product Cost (Input)", 0) or 0
                R = variant.get("Delivery Rate", 0) or 0

                # Write values
                worksheet.write(variant_row_idx, 0, "", current_variant_format)
                worksheet.write(variant_row_idx, 1, variant.get("Product variant title", ""), current_variant_format)
                worksheet.write(variant_row_idx, 2, P, current_variant_format)
                worksheet.write(variant_row_idx, 3, S, current_variant_format)
                worksheet.write(variant_row_idx, 4, A_usd, current_variant_format)
                worksheet.write(variant_row_idx, 5, C, current_variant_format)
                worksheet.write(variant_row_idx, 6, R, current_variant_format)

                # Score formula
                formula = f'''=IF(AND(C{excel_row}>0,D{excel_row}>0),
                    ((C{excel_row}*D{excel_row}*IF(G{excel_row}>1,G{excel_row}/100,G{excel_row}))
                    -(E{excel_row}*100)-(77*D{excel_row})-(65*D{excel_row})
                    -(F{excel_row}*D{excel_row}*IF(G{excel_row}>1,G{excel_row}/100,G{excel_row})))
                    /((C{excel_row}*D{excel_row}*IF(G{excel_row}>1,G{excel_row}/100,G{excel_row}))*0.1),0)'''
                worksheet.write_formula(variant_row_idx, 7, formula, current_variant_format)

                row += 1

            last_variant_row = row - 1

            # Product totals formulas
            excel_first = first_variant_row + 1
            excel_last = last_variant_row + 1
            worksheet.write_formula(product_total_row_idx, 3, f"=SUM(D{excel_first}:D{excel_last})", current_product_format)  # Net items sold
            worksheet.write_formula(product_total_row_idx, 4, f"=SUM(E{excel_first}:E{excel_last})", current_product_format)  # Ad Spend USD

            # Weighted averages
            worksheet.write_formula(product_total_row_idx, 2,
                f"=IF(SUM(D{excel_first}:D{excel_last})=0,0,"
                f"SUMPRODUCT(C{excel_first}:C{excel_last},D{excel_first}:D{excel_last})/SUM(D{excel_first}:D{excel_last}))",
                current_product_format)

            worksheet.write_formula(product_total_row_idx, 5,
                f"=IF(SUM(D{excel_first}:D{excel_last})=0,0,"
                f"SUMPRODUCT(F{excel_first}:F{excel_last},D{excel_first}:D{excel_last})/SUM(D{excel_first}:D{excel_last}))",
                current_product_format)

            worksheet.write_formula(product_total_row_idx, 6,
                f"=IF(SUM(D{excel_first}:D{excel_last})=0,0,"
                f"SUMPRODUCT(G{excel_first}:G{excel_last},D{excel_first}:D{excel_last})/SUM(D{excel_first}:D{excel_last}))",
                current_product_format)

            # Score formula for product totals
            product_excel_row = product_total_row_idx + 1
            score_formula = f'''=IF(AND(C{product_excel_row}>0,D{product_excel_row}>0),
                ((C{product_excel_row}*D{product_excel_row}*IF(G{product_excel_row}>1,G{product_excel_row}/100,G{product_excel_row}))
                -(E{product_excel_row}*100)-(77*D{product_excel_row})-(65*D{product_excel_row})
                -(F{product_excel_row}*D{product_excel_row}*IF(G{product_excel_row}>1,G{product_excel_row}/100,G{product_excel_row})))
                /((C{product_excel_row}*D{product_excel_row}*IF(G{product_excel_row}>1,G{product_excel_row}/100,G{product_excel_row}))*0.1),0)'''
            worksheet.write_formula(product_total_row_idx, 7, score_formula, current_product_format)

        # Grand total formulas
        if product_total_rows:
            excel_rows = [str(row_idx + 1) for row_idx in product_total_rows]
            
            # Sums
            worksheet.write_formula(grand_total_row, 3, f"=SUM({','.join([f'D{row}' for row in excel_rows])})", grand_total_format)
            worksheet.write_formula(grand_total_row, 4, f"=SUM({','.join([f'E{row}' for row in excel_rows])})", grand_total_format)

            # Find all variant rows for weighted averages
            all_variant_rows = []
            current_row = grand_total_row + 1
            for product, product_df in df.groupby("Product title"):
                current_row += 1  # Skip product total row
                for _ in range(len(product_df)):
                    all_variant_rows.append(current_row)
                    current_row += 1

            if all_variant_rows:
                first_variant_excel = all_variant_rows[0]
                last_variant_excel = all_variant_rows[-1]

                # Weighted averages
                worksheet.write_formula(grand_total_row, 2,
                    f"=IF(SUM(D{first_variant_excel}:D{last_variant_excel})=0,0,"
                    f"SUMPRODUCT(C{first_variant_excel}:C{last_variant_excel},D{first_variant_excel}:D{last_variant_excel})/SUM(D{first_variant_excel}:D{last_variant_excel}))",
                    grand_total_format)

                worksheet.write_formula(grand_total_row, 5,
                    f"=IF(SUM(D{first_variant_excel}:D{last_variant_excel})=0,0,"
                    f"SUMPRODUCT(F{first_variant_excel}:F{last_variant_excel},D{first_variant_excel}:D{last_variant_excel})/SUM(D{first_variant_excel}:D{last_variant_excel}))",
                    grand_total_format)

                worksheet.write_formula(grand_total_row, 6,
                    f"=IF(SUM(D{first_variant_excel}:D{last_variant_excel})=0,0,"
                    f"SUMPRODUCT(G{first_variant_excel}:G{last_variant_excel},D{first_variant_excel}:D{last_variant_excel})/SUM(D{first_variant_excel}:D{last_variant_excel}))",
                    grand_total_format)

            # Grand total score
            gt_excel_row = grand_total_row + 1
            score_formula = f'''=IF(AND(C{gt_excel_row}>0,D{gt_excel_row}>0),
                ((C{gt_excel_row}*D{gt_excel_row}*IF(G{gt_excel_row}>1,G{gt_excel_row}/100,G{gt_excel_row}))
                -(E{gt_excel_row}*100)-(77*D{gt_excel_row})-(65*D{gt_excel_row})
                -(F{gt_excel_row}*D{gt_excel_row}*IF(G{gt_excel_row}>1,G{gt_excel_row}/100,G{gt_excel_row})))
                /((C{gt_excel_row}*D{gt_excel_row}*IF(G{gt_excel_row}>1,G{gt_excel_row}/100,G{gt_excel_row}))*0.1),0)'''
            worksheet.write_formula(grand_total_row, 7, score_formula, grand_total_format)

        worksheet.freeze_panes(2, 0)
        for i, col in enumerate(visible_cols):
            if col in ("Product title", "Product variant title"):
                worksheet.set_column(i, i, 35)
            else:
                worksheet.set_column(i, i, 15)

    return output.getvalue()




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
        
        # Check if we have dates
        has_dates = 'Date' in df.columns
        if not has_dates:
            # Fall back to simple structure if no dates
            return convert_shopify_to_excel_staff_simple(df)
        
        # Get unique dates and sort them
        unique_dates = sorted([str(d) for d in df['Date'].unique() if pd.notna(d) and str(d).strip() != ''])
        num_days = len(unique_dates)
        
        # Calculate dynamic threshold
        dynamic_threshold = num_days * 5
        
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
                    {'level': group_level, 'collapsed': True}
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
                
                # Calculate "B.E" for base column (Cost per item at break-even)
                total_avg_price_col_idx = all_columns.index("Total_Avg Price")
                total_delivery_rate_col_idx = all_columns.index("Total_Delivery Rate")
                total_product_cost_col_idx = all_columns.index("Total_Product Cost Input")
                
                total_avg_price_ref = f"{xl_col_to_name(total_avg_price_col_idx)}{excel_row}"
                total_net_items_ref = f"{xl_col_to_name(total_net_items_col_idx)}{excel_row}"
                total_delivery_rate_ref = f"{xl_col_to_name(total_delivery_rate_col_idx)}{excel_row}"
                total_product_cost_ref = f"{xl_col_to_name(total_product_cost_col_idx)}{excel_row}"
                
                break_even_formula = f'''=IF(AND({total_avg_price_ref}>0,{total_net_items_ref}>0),
                    ((({total_avg_price_ref}*{total_net_items_ref}*IF(ISNUMBER({total_delivery_rate_ref}),IF({total_delivery_rate_ref}>1,{total_delivery_rate_ref}/100,{total_delivery_rate_ref}),0))
                    -(77*{total_net_items_ref})-(65*{total_net_items_ref})
                    -({total_product_cost_ref}*{total_net_items_ref}*IF(ISNUMBER({total_delivery_rate_ref}),IF({total_delivery_rate_ref}>1,{total_delivery_rate_ref}/100,{total_delivery_rate_ref}),0)))/100)/{total_net_items_ref},0)'''
                
                worksheet.write_formula(
                    variant_row_idx, 7,
                    break_even_formula,
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



def convert_final_campaign_to_excel_staff_with_date_columns_fixed(df, shopify_df=None):
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

        # Check if we have dates
        has_dates = 'Date' in df.columns
        if not has_dates:
            # Fall back to simple structure if no dates
            return convert_final_campaign_to_excel(df, [])
        
        # Get unique dates and sort them
        unique_dates = sorted([str(d) for d in df['Date'].unique() if pd.notna(d) and str(d).strip() != ''])
        
        # Define base columns for staff (CHANGED: "Cost Per Purchase" to "C.P.P" and "Break Even Point" to "B.E")
        base_columns = ["Product Name", "Campaign Name", "Total Amount Spent (USD)", "Purchases", "C.P.P", "B.E"]
        
        # Define staff metrics (simplified - only 6 metrics per date for campaigns)
        # CHANGED: "Cost Per Purchase (USD)" to "C.P.P (USD)"
        date_metrics = ["Amount Spent (USD)", "Purchases", "C.P.P (USD)", "Avg Price", "Delivery Rate", "Score"]
        
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

        # Track campaigns for unmatched sheet analysis
        matched_campaigns = []
        unmatched_campaigns = []

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
                    {'level': group_level, 'collapsed': True}
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
        safe_write(worksheet, grand_total_row_idx, 0, "ALL PRODUCTS", grand_total_format)
        safe_write(worksheet, grand_total_row_idx, 1, "GRAND TOTAL", grand_total_format)
        
        # Add empty remark for grand total
        remark_col_idx = all_columns.index("Remark")
        safe_write(worksheet, grand_total_row_idx, remark_col_idx, "", remark_format)
        
        # Add empty remark for grand total
        remark_col_idx = all_columns.index("Remark")
        safe_write(worksheet, grand_total_row_idx, remark_col_idx, "", remark_format)

        row = grand_total_row_idx + 1
        product_total_rows = []

        # Group by product and restructure data
        for product, product_df in df.groupby("Product"):
            # Check if this product has Shopify data (day-wise lookups)
            has_shopify_data = (product in product_date_avg_prices and 
                              any(date in product_date_avg_prices[product] for date in unique_dates) or
                              product in product_date_delivery_rates and 
                              any(date in product_date_delivery_rates[product] for date in unique_dates) or
                              product in product_date_cost_inputs and 
                              any(date in product_date_cost_inputs[product] for date in unique_dates))
            
            product_total_row_idx = row
            product_total_rows.append(product_total_row_idx)

            # Calculate totals for this product
            total_amount_spent_for_product = product_df["Amount Spent (USD)"].sum()
            total_purchases_for_product = product_df["Purchases"].sum()

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
            
            # Add remark for products with zero total amount spent
            remark_col_idx = all_columns.index("Remark")
            if total_amount_spent_for_product == 0:
                safe_write(worksheet, product_total_row_idx, remark_col_idx, "Issue with this campaign data - zero spending", remark_format)
            else:
                safe_write(worksheet, product_total_row_idx, remark_col_idx, "", remark_format)

            # NEW: Pre-calculate cost per purchase for each campaign to enable sorting
            campaign_cpp_list = []
            for campaign_name, campaign_group in product_df.groupby("Campaign Name"):
                total_amount_spent_usd = campaign_group.get("Amount Spent (USD)", 0).sum() if "Amount Spent (USD)" in campaign_group.columns else 0
                total_purchases = campaign_group.get("Purchases", 0).sum() if "Purchases" in campaign_group.columns else 0
                
                # Calculate cost per purchase for sorting
                cost_per_purchase = 0
                if total_purchases > 0:
                    cost_per_purchase = total_amount_spent_usd / total_purchases
                
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
                
                campaign_cpp_list.append(campaign_info)
                
                if has_shopify_data:
                    matched_campaigns.append(campaign_info)
                else:
                    unmatched_campaigns.append(campaign_info)
            
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
                
                # Add remark for campaigns (empty for individual campaigns)
                remark_col_idx = all_columns.index("Remark")
                safe_write(worksheet, campaign_row_idx, remark_col_idx, "", remark_format)
                
                # Cell references for Excel formulas
                excel_row = campaign_row_idx + 1
                
                # Fill date-specific data and formulas
                for date in unique_dates:
                    date_data = campaign_group[campaign_group['Date'].astype(str) == date]
                    
                    # Get column indices for this date (UPDATED: using "C.P.P (USD)")
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
                    
                    # Average Price - from day-wise lookup for this product and date
                    date_avg_price = product_date_avg_prices.get(product, {}).get(date, 0)
                    safe_write(worksheet, campaign_row_idx, avg_price_col_idx, round(float(date_avg_price), 2), campaign_format)
                    
                    # Delivery Rate - from day-wise lookup for this product and date
                    date_delivery_rate = product_date_delivery_rates.get(product, {}).get(date, 0)
                    safe_write(worksheet, campaign_row_idx, delivery_rate_col_idx, round(float(date_delivery_rate), 2), campaign_format)
                    
                    if not date_data.empty:
                        row_data = date_data.iloc[0]
                        
                        # Amount Spent (USD) - from campaign data
                        amount_spent = row_data.get("Amount Spent (USD)", 0) or 0
                        safe_write(worksheet, campaign_row_idx, amount_spent_col_idx, round(float(amount_spent), 2), campaign_format)
                        
                        # Purchases - from campaign data  
                        purchases = row_data.get("Purchases", 0) or 0
                        safe_write(worksheet, campaign_row_idx, purchases_col_idx, purchases, campaign_format)
                        
                    else:
                        # No data for this date
                        safe_write(worksheet, campaign_row_idx, amount_spent_col_idx, 0, campaign_format)
                        safe_write(worksheet, campaign_row_idx, purchases_col_idx, 0, campaign_format)
                    
                    # FORMULAS for calculated fields
                    
                    # C.P.P (USD) = Amount Spent (USD) / Purchases
                    worksheet.write_formula(
                        campaign_row_idx, cost_per_purchase_col_idx,
                        f"=IF({purchases_ref}=0,0,{amount_spent_ref}/{purchases_ref})",
                        campaign_format
                    )
                    
                    # SCORE FORMULA for staff (corrected to match first code)
                    rate_term = f"IF(ISNUMBER({delivery_rate_ref}),IF({delivery_rate_ref}>1,{delivery_rate_ref}/100,{delivery_rate_ref}),0)"
                    
                    # Get product cost from day-wise lookup for this product and date  
                    date_product_cost = product_date_cost_inputs.get(product, {}).get(date, 0)
                    
                    score_formula = f'''=IF(AND({avg_price_ref}>0,{purchases_ref}>0),
                        (({avg_price_ref}*{purchases_ref}*{rate_term})
                        -({amount_spent_ref}*100)-(77*{purchases_ref})-(65*{purchases_ref})
                        -({date_product_cost}*{purchases_ref}*{rate_term}))
                        /(({avg_price_ref}*{purchases_ref}*{rate_term})*0.1),0)'''
                    
                    worksheet.write_formula(
                        campaign_row_idx, score_col_idx,
                        score_formula,
                        campaign_format
                    )
                
                # TOTAL COLUMNS CALCULATIONS FOR CAMPAIGN
                for metric in date_metrics:
                    total_col_idx = all_columns.index(f"Total_{metric}")
                    
                    if metric in ["Avg Price", "Delivery Rate"]:
                        # WEIGHTED AVERAGE based on purchases
                        total_purchases_col_idx = all_columns.index("Total_Purchases")
                        total_purchases_ref = f"{xl_col_to_name(total_purchases_col_idx)}{excel_row}"
                        
                        if len(unique_dates) > 1:
                            metric_terms = []
                            for date in unique_dates:
                                metric_col_idx = all_columns.index(f"{date}_{metric}")
                                purchases_col_idx = all_columns.index(f"{date}_Purchases")
                                metric_terms.append(f"{xl_col_to_name(metric_col_idx)}{excel_row}*{xl_col_to_name(purchases_col_idx)}{excel_row}")
                            
                            sumproduct_formula = "+".join(metric_terms)
                            worksheet.write_formula(
                                campaign_row_idx, total_col_idx,
                                f"=IF({total_purchases_ref}=0,0,({sumproduct_formula})/{total_purchases_ref})",
                                campaign_format
                            )
                        else:
                            single_date_col = all_columns.index(f"{unique_dates[0]}_{metric}")
                            worksheet.write_formula(
                                campaign_row_idx, total_col_idx,
                                f"={xl_col_to_name(single_date_col)}{excel_row}",
                                campaign_format
                            )
                    
                    elif metric == "C.P.P (USD)":  # UPDATED: changed from "Cost Per Purchase (USD)"
                        # CALCULATED: Total Amount Spent / Total Purchases
                        total_amount_spent_col_idx = all_columns.index("Total_Amount Spent (USD)")
                        total_purchases_col_idx = all_columns.index("Total_Purchases")
                        total_amount_spent_ref = f"{xl_col_to_name(total_amount_spent_col_idx)}{excel_row}"
                        total_purchases_ref = f"{xl_col_to_name(total_purchases_col_idx)}{excel_row}"
                        
                        worksheet.write_formula(
                            campaign_row_idx, total_col_idx,
                            f"=IF({total_purchases_ref}=0,0,{total_amount_spent_ref}/{total_purchases_ref})",
                            campaign_format
                        )
                    
                    elif metric == "Score":
                        # TOTAL SCORE FORMULA
                        total_avg_price_col_idx = all_columns.index("Total_Avg Price")
                        total_purchases_col_idx = all_columns.index("Total_Purchases")
                        total_amount_spent_col_idx = all_columns.index("Total_Amount Spent (USD)")
                        total_delivery_rate_col_idx = all_columns.index("Total_Delivery Rate")
                        
                        total_avg_price_ref = f"{xl_col_to_name(total_avg_price_col_idx)}{excel_row}"
                        total_purchases_ref = f"{xl_col_to_name(total_purchases_col_idx)}{excel_row}"
                        total_amount_spent_ref = f"{xl_col_to_name(total_amount_spent_col_idx)}{excel_row}"
                        total_delivery_rate_ref = f"{xl_col_to_name(total_delivery_rate_col_idx)}{excel_row}"
                        
                        total_rate_term = f"IF(ISNUMBER({total_delivery_rate_ref}),IF({total_delivery_rate_ref}>1,{total_delivery_rate_ref}/100,{total_delivery_rate_ref}),0)"
                        
                        # Get average product cost for this product across all dates
                        product_costs = []
                        for date in unique_dates:
                            date_cost = product_date_cost_inputs.get(product, {}).get(date, 0)
                            if date_cost > 0:
                                product_costs.append(date_cost)
                        avg_product_cost = sum(product_costs) / len(product_costs) if product_costs else 0
                        
                        total_score_formula = f'''=IF(AND({total_avg_price_ref}>0,{total_purchases_ref}>0),
                            (({total_avg_price_ref}*{total_purchases_ref}*{total_rate_term})
                            -({total_amount_spent_ref}*100)-(77*{total_purchases_ref})-(65*{total_purchases_ref})
                            -({avg_product_cost}*{total_purchases_ref}*{total_rate_term}))
                            /(({total_avg_price_ref}*{total_purchases_ref}*{total_rate_term})*0.1),0)'''
                        
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
                total_cost_per_purchase_col_idx = all_columns.index("Total_C.P.P (USD)")  # UPDATED
                
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
                
                # NEW "B.E" FORMULA - Cost per item at zero score
                # B.E = (Zero Score Amount Spent) / Purchases
                
                total_avg_price_col_idx = all_columns.index("Total_Avg Price")
                total_delivery_rate_col_idx = all_columns.index("Total_Delivery Rate")
                
                total_avg_price_ref = f"{xl_col_to_name(total_avg_price_col_idx)}{excel_row}"
                total_purchases_ref = f"{xl_col_to_name(total_purchases_col_idx)}{excel_row}"
                total_delivery_rate_ref = f"{xl_col_to_name(total_delivery_rate_col_idx)}{excel_row}"
                
                # Get average product cost for this product across all dates
                product_costs = []
                for date in unique_dates:
                    date_cost = product_date_cost_inputs.get(product, {}).get(date, 0)
                    if date_cost > 0:
                        product_costs.append(date_cost)
                avg_product_cost = sum(product_costs) / len(product_costs) if product_costs else 0
                
                # B.E FORMULA: Zero Score Amount Spent ÷ Purchases
                # First calculate the zero score amount spent, then divide by purchases
                break_even_formula = f'''=IF(AND({total_avg_price_ref}>0,{total_purchases_ref}>0),
                    (({total_avg_price_ref}*{total_purchases_ref}*IF(ISNUMBER({total_delivery_rate_ref}),IF({total_delivery_rate_ref}>1,{total_delivery_rate_ref}/100,{total_delivery_rate_ref}),0))
                    -(77*{total_purchases_ref})-(65*{total_purchases_ref})
                    -({avg_product_cost}*{total_purchases_ref}*IF(ISNUMBER({total_delivery_rate_ref}),IF({total_delivery_rate_ref}>1,{total_delivery_rate_ref}/100,{total_delivery_rate_ref}),0)))/100/{total_purchases_ref},0)'''
                
                worksheet.write_formula(
                    campaign_row_idx, 5,
                    break_even_formula,
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
                        
                        if metric in ["Avg Price", "Delivery Rate"]:
                            # Weighted average based on purchases for this date using RANGES
                            date_purchases_col_idx = all_columns.index(f"{date}_Purchases")
                            
                            metric_range = f"{xl_col_to_name(col_idx)}{first_campaign_row}:{xl_col_to_name(col_idx)}{last_campaign_row}"
                            purchases_range = f"{xl_col_to_name(date_purchases_col_idx)}{first_campaign_row}:{xl_col_to_name(date_purchases_col_idx)}{last_campaign_row}"
                            
                            worksheet.write_formula(
                                product_total_row_idx, col_idx,
                                f"=IF(SUM({purchases_range})=0,0,SUMPRODUCT({metric_range},{purchases_range})/SUM({purchases_range}))",
                                product_total_format
                            )
                        elif metric == "C.P.P (USD)":  # UPDATED: changed from "Cost Per Purchase (USD)"
                            # Calculate based on totals for this date
                            amount_spent_idx = all_columns.index(f"{date}_Amount Spent (USD)")
                            purchases_idx = all_columns.index(f"{date}_Purchases")
                            worksheet.write_formula(
                                product_total_row_idx, col_idx,
                                f"=IF({xl_col_to_name(purchases_idx)}{product_total_row_idx+1}=0,0,{xl_col_to_name(amount_spent_idx)}{product_total_row_idx+1}/{xl_col_to_name(purchases_idx)}{product_total_row_idx+1})",
                                product_total_format
                            )
                        elif metric == "Score":
                            # CORRECT SCORE CALCULATION FOR PRODUCT TOTAL (using aggregated values)
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
                            
                            score_formula = f'''=IF(AND({avg_price_ref}>0,{purchases_ref}>0),
                                (({avg_price_ref}*{purchases_ref}*{rate_term})
                                -({amount_spent_ref}*100)-(77*{purchases_ref})-(65*{purchases_ref})
                                -({date_product_cost}*{purchases_ref}*{rate_term}))
                                /(({avg_price_ref}*{purchases_ref}*{rate_term})*0.1),0)'''
                            
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
                        # Weighted average based on total purchases using RANGES
                        total_purchases_col_idx = all_columns.index("Total_Purchases")
                        
                        metric_range = f"{xl_col_to_name(col_idx)}{first_campaign_row}:{xl_col_to_name(col_idx)}{last_campaign_row}"
                        purchases_range = f"{xl_col_to_name(total_purchases_col_idx)}{first_campaign_row}:{xl_col_to_name(total_purchases_col_idx)}{last_campaign_row}"
                        
                        worksheet.write_formula(
                            product_total_row_idx, col_idx,
                            f"=IF(SUM({purchases_range})=0,0,SUMPRODUCT({metric_range},{purchases_range})/SUM({purchases_range}))",
                            product_total_format
                        )
                    elif metric == "C.P.P (USD)":  # UPDATED: changed from "Cost Per Purchase (USD)"
                        # Calculate based on totals
                        total_amount_spent_idx = all_columns.index("Total_Amount Spent (USD)")
                        total_purchases_idx = all_columns.index("Total_Purchases")
                        worksheet.write_formula(
                            product_total_row_idx, col_idx,
                            f"=IF({xl_col_to_name(total_purchases_idx)}{product_total_row_idx+1}=0,0,{xl_col_to_name(total_amount_spent_idx)}{product_total_row_idx+1}/{xl_col_to_name(total_purchases_idx)}{product_total_row_idx+1})",
                            product_total_format
                        )
                    elif metric == "Score":
                        # CORRECT SCORE CALCULATION FOR PRODUCT TOTAL (using aggregated total values)
                        total_avg_price_idx = all_columns.index("Total_Avg Price")
                        total_purchases_idx = all_columns.index("Total_Purchases")
                        total_amount_spent_idx = all_columns.index("Total_Amount Spent (USD)")
                        total_delivery_rate_idx = all_columns.index("Total_Delivery Rate")
                        
                        avg_price_ref = f"{xl_col_to_name(total_avg_price_idx)}{product_total_row_idx+1}"
                        purchases_ref = f"{xl_col_to_name(total_purchases_idx)}{product_total_row_idx+1}"
                        amount_spent_ref = f"{xl_col_to_name(total_amount_spent_idx)}{product_total_row_idx+1}"
                        delivery_rate_ref = f"{xl_col_to_name(total_delivery_rate_idx)}{product_total_row_idx+1}"
                        
                        rate_term = f"IF(ISNUMBER({delivery_rate_ref}),IF({delivery_rate_ref}>1,{delivery_rate_ref}/100,{delivery_rate_ref}),0)"
                        
                        # Get average product cost for this product across all dates
                        product_costs = []
                        for date in unique_dates:
                            date_cost = product_date_cost_inputs.get(product, {}).get(date, 0)
                            if date_cost > 0:
                                product_costs.append(date_cost)
                        avg_product_cost = sum(product_costs) / len(product_costs) if product_costs else 0
                        
                        score_formula = f'''=IF(AND({avg_price_ref}>0,{purchases_ref}>0),
                            (({avg_price_ref}*{purchases_ref}*{rate_term})
                            -({amount_spent_ref}*100)-(77*{purchases_ref})-(65*{purchases_ref})
                            -({avg_product_cost}*{purchases_ref}*{rate_term}))
                            /(({avg_price_ref}*{purchases_ref}*{rate_term})*0.1),0)'''
                        
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
                total_cost_per_purchase_col_idx = all_columns.index("Total_C.P.P (USD)")  # UPDATED
                
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
                
                # "B.E" for product total
                total_avg_price_col_idx = all_columns.index("Total_Avg Price")
                total_delivery_rate_col_idx = all_columns.index("Total_Delivery Rate")
                
                total_avg_price_ref = f"{xl_col_to_name(total_avg_price_col_idx)}{product_total_row_idx+1}"
                total_purchases_ref = f"{xl_col_to_name(total_purchases_col_idx)}{product_total_row_idx+1}"
                total_delivery_rate_ref = f"{xl_col_to_name(total_delivery_rate_col_idx)}{product_total_row_idx+1}"
                
                # Get average product cost for this product across all dates
                product_costs = []
                for date in unique_dates:
                    date_cost = product_date_cost_inputs.get(product, {}).get(date, 0)
                    if date_cost > 0:
                        product_costs.append(date_cost)
                avg_product_cost = sum(product_costs) / len(product_costs) if product_costs else 0
                
                break_even_formula = f'''=IF(AND({total_avg_price_ref}>0,{total_purchases_ref}>0),
                    (({total_avg_price_ref}*{total_purchases_ref}*IF(ISNUMBER({total_delivery_rate_ref}),IF({total_delivery_rate_ref}>1,{total_delivery_rate_ref}/100,{total_delivery_rate_ref}),0))
                    -(77*{total_purchases_ref})-(65*{total_purchases_ref})
                    -({avg_product_cost}*{total_purchases_ref}*IF(ISNUMBER({total_delivery_rate_ref}),IF({total_delivery_rate_ref}>1,{total_delivery_rate_ref}/100,{total_delivery_rate_ref}),0)))/100/{total_purchases_ref},0)'''
                
                worksheet.write_formula(
                    product_total_row_idx, 5,
                    break_even_formula,
                    product_total_format
                )

        # Calculate grand totals using INDIVIDUAL PRODUCT TOTAL ROWS ONLY
        if product_total_rows:
            # Add base columns to grand total
            total_amount_spent_col_idx = all_columns.index("Total_Amount Spent (USD)")
            total_purchases_col_idx = all_columns.index("Total_Purchases")
            total_cost_per_purchase_col_idx = all_columns.index("Total_C.P.P (USD)")  # UPDATED
            
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
            
            # "B.E" for grand total
            total_avg_price_col_idx = all_columns.index("Total_Avg Price")
            total_delivery_rate_col_idx = all_columns.index("Total_Delivery Rate")
            
            total_avg_price_ref = f"{xl_col_to_name(total_avg_price_col_idx)}{grand_total_row_idx+1}"
            total_purchases_ref = f"{xl_col_to_name(total_purchases_col_idx)}{grand_total_row_idx+1}"
            total_delivery_rate_ref = f"{xl_col_to_name(total_delivery_rate_col_idx)}{grand_total_row_idx+1}"
            
            # Calculate average product cost across all products and dates
            all_product_costs = []
            for prod in df['Product'].unique():
                for dt in unique_dates:
                    cost = product_date_cost_inputs.get(prod, {}).get(dt, 0)
                    if cost > 0:
                        all_product_costs.append(cost)
            grand_avg_product_cost = sum(all_product_costs) / len(all_product_costs) if all_product_costs else 0
            
            break_even_formula = f'''=IF(AND({total_avg_price_ref}>0,{total_purchases_ref}>0),
                (({total_avg_price_ref}*{total_purchases_ref}*IF(ISNUMBER({total_delivery_rate_ref}),IF({total_delivery_rate_ref}>1,{total_delivery_rate_ref}/100,{total_delivery_rate_ref}),0))
                -(77*{total_purchases_ref})-(65*{total_purchases_ref})
                -({grand_avg_product_cost}*{total_purchases_ref}*IF(ISNUMBER({total_delivery_rate_ref}),IF({total_delivery_rate_ref}>1,{total_delivery_rate_ref}/100,{total_delivery_rate_ref}),0)))/100/{total_purchases_ref},0)'''
            
            worksheet.write_formula(
                grand_total_row_idx, 5,
                break_even_formula,
                grand_total_format
            )
            
            # Date-specific and total columns for grand total using INDIVIDUAL PRODUCT ROWS
            for date in unique_dates:
                for metric in date_metrics:
                    col_idx = all_columns.index(f"{date}_{metric}")
                    
                    if metric in ["Avg Price", "Delivery Rate"]:
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
                    elif metric == "C.P.P (USD)":  # UPDATED: changed from "Cost Per Purchase (USD)"
                        # Calculate based on totals for this date
                        amount_spent_idx = all_columns.index(f"{date}_Amount Spent (USD)")
                        purchases_idx = all_columns.index(f"{date}_Purchases")
                        worksheet.write_formula(
                            grand_total_row_idx, col_idx,
                            f"=IF({xl_col_to_name(purchases_idx)}{grand_total_row_idx+1}=0,0,{xl_col_to_name(amount_spent_idx)}{grand_total_row_idx+1}/{xl_col_to_name(purchases_idx)}{grand_total_row_idx+1})",
                            grand_total_format
                        )
                    elif metric == "Score":
                        # CORRECT SCORE CALCULATION FOR GRAND TOTAL (using aggregated values)
                        avg_price_idx = all_columns.index(f"{date}_Avg Price")
                        purchases_idx = all_columns.index(f"{date}_Purchases")
                        amount_spent_idx = all_columns.index(f"{date}_Amount Spent (USD)")
                        delivery_rate_idx = all_columns.index(f"{date}_Delivery Rate")
                        
                        avg_price_ref = f"{xl_col_to_name(avg_price_idx)}{grand_total_row_idx+1}"
                        purchases_ref = f"{xl_col_to_name(purchases_idx)}{grand_total_row_idx+1}"
                        amount_spent_ref = f"{xl_col_to_name(amount_spent_idx)}{grand_total_row_idx+1}"
                        delivery_rate_ref = f"{xl_col_to_name(delivery_rate_idx)}{grand_total_row_idx+1}"
                        
                        rate_term = f"IF(ISNUMBER({delivery_rate_ref}),IF({delivery_rate_ref}>1,{delivery_rate_ref}/100,{delivery_rate_ref}),0)"
                        
                        # Calculate average product cost across all products and dates
                        all_product_costs = []
                        for prod in df['Product'].unique():
                            for dt in unique_dates:
                                cost = product_date_cost_inputs.get(prod, {}).get(dt, 0)
                                if cost > 0:
                                    all_product_costs.append(cost)
                        grand_avg_product_cost = sum(all_product_costs) / len(all_product_costs) if all_product_costs else 0
                        
                        score_formula = f'''=IF(AND({avg_price_ref}>0,{purchases_ref}>0),
                            (({avg_price_ref}*{purchases_ref}*{rate_term})
                            -({amount_spent_ref}*100)-(77*{purchases_ref})-(65*{purchases_ref})
                            -({grand_avg_product_cost}*{purchases_ref}*{rate_term}))
                            /(({avg_price_ref}*{purchases_ref}*{rate_term})*0.1),0)'''
                        
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
            total_purchases_col_idx = all_columns.index("Total_Purchases")
            
            for metric in date_metrics:
                col_idx = all_columns.index(f"Total_{metric}")
                
                if metric in ["Avg Price", "Delivery Rate"]:
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
                elif metric == "C.P.P (USD)":  # UPDATED: changed from "Cost Per Purchase (USD)"
                    # Calculate based on totals
                    total_amount_spent_idx = all_columns.index("Total_Amount Spent (USD)")
                    total_purchases_idx = all_columns.index("Total_Purchases")
                    worksheet.write_formula(
                        grand_total_row_idx, col_idx,
                        f"=IF({xl_col_to_name(total_purchases_idx)}{grand_total_row_idx+1}=0,0,{xl_col_to_name(total_amount_spent_idx)}{grand_total_row_idx+1}/{xl_col_to_name(total_purchases_idx)}{grand_total_row_idx+1})",
                        grand_total_format
                    )
                elif metric == "Score":
                    # CORRECT SCORE CALCULATION FOR GRAND TOTAL (using aggregated total values)
                    total_avg_price_idx = all_columns.index("Total_Avg Price")
                    total_purchases_idx = all_columns.index("Total_Purchases")
                    total_amount_spent_idx = all_columns.index("Total_Amount Spent (USD)")
                    total_delivery_rate_idx = all_columns.index("Total_Delivery Rate")
                    
                    avg_price_ref = f"{xl_col_to_name(total_avg_price_idx)}{grand_total_row_idx+1}"
                    purchases_ref = f"{xl_col_to_name(total_purchases_idx)}{grand_total_row_idx+1}"
                    amount_spent_ref = f"{xl_col_to_name(total_amount_spent_idx)}{grand_total_row_idx+1}"
                    delivery_rate_ref = f"{xl_col_to_name(total_delivery_rate_idx)}{grand_total_row_idx+1}"
                    
                    rate_term = f"IF(ISNUMBER({delivery_rate_ref}),IF({delivery_rate_ref}>1,{delivery_rate_ref}/100,{delivery_rate_ref}),0)"
                    score_formula = f'''=IF(AND({avg_price_ref}>0,{purchases_ref}>0),
                        (({avg_price_ref}*{purchases_ref}*{rate_term})
                        -({amount_spent_ref}*100)-(77*{purchases_ref})-(65*{purchases_ref}))
                        /(({avg_price_ref}*{purchases_ref}*{rate_term})*0.1),0)'''
                    
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
                cost_per_purchase_usd = 0
                if campaign['Purchases'] > 0:
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
                cost_per_purchase_usd = 0
                if campaign['Purchases'] > 0:
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
        
    return output.getvalue()




st.header("📥 Download Processed Files")

# ---- SHOPIFY DOWNLOAD ----
if df_shopify is not None:
    export_df = df_shopify.drop(columns=["Product Name", "Canonical Product"], errors="ignore")

    # Use simple structure for staff version
    shopify_excel = convert_shopify_to_excel_staff_with_date_columns_corrected(export_df)
    st.download_button(
        label="📥 Download Staff Shopify File (Excel)",
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

    # Download processed campaign data (simple format)
    if grouped_campaign is not None:
        excel_data = convert_df_to_excel(grouped_campaign)
        st.download_button(
            label="📥 Download Processed Campaign File (Excel)",
            data=excel_data,
            file_name="staff_processed_campaigns.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    # Download final campaign data (structured format for staff)
    if 'df_final_campaign' in locals() and not df_final_campaign.empty:
        final_campaign_excel = convert_final_campaign_to_excel_staff_with_date_columns_fixed(df_final_campaign)
        if final_campaign_excel:
            st.download_button(
                label="🎯 Download Staff Campaign File (Structured Excel)",
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
        
        
        
        
    
        




