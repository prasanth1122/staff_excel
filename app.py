import streamlit as st
import pandas as pd
from io import BytesIO
import re
from rapidfuzz import fuzz, process
from xlsxwriter.utility import xl_col_to_name
import math

st.title("📊 Campaign + Shopify Data Processor")

# ---- UPLOAD ----
campaign_file = st.file_uploader("Upload Campaign Data (Excel/CSV)", type=["xlsx", "csv"])
shopify_file = st.file_uploader("Upload Shopify Data (Excel/CSV)", type=["xlsx", "csv"])

# ---- UPLOAD OLD MERGED DATA ----
st.subheader("📋 Import Delivery Rates & Product Costs from Previous Data (Optional)")
old_merged_file = st.file_uploader(
    "Upload Old Merged Data (Excel/CSV) - to import delivery rates and product costs",
    type=["xlsx", "csv"],
    help="Upload your previous merged data file to automatically import delivery rates and product costs for matching products"
)

# ---- HELPERS ----
def safe_write1(worksheet, row, col, value, cell_format=None):
    """Wrapper around worksheet.write to handle NaN/inf safely"""
    if isinstance(value, (int, float)):
        if value is None or (isinstance(value, float) and (math.isnan(value) or math.isinf(value))):
            value = 0
    else:
        if pd.isna(value):
            value = ""
    worksheet.write(row, col, value, cell_format)

# ---- STATE ----
df_campaign, df_shopify, df_old_merged = None, None, None
grouped_campaign = None



# ---- LOAD OLD MERGED DATA ----
if old_merged_file:
    try:
        if old_merged_file.name.endswith(".csv"):
            df_old_merged = pd.read_csv(old_merged_file)
        else:
            df_old_merged = pd.read_excel(old_merged_file)

        required_old_cols = ["Product title", "Product variant title", "Delivery Rate"]
        if all(col in df_old_merged.columns for col in required_old_cols):
            current_product = None
            for idx, row in df_old_merged.iterrows():
                if pd.notna(row["Product title"]) and row["Product title"].strip() != "":
                    if row["Product variant title"] == "ALL VARIANTS (TOTAL)":
                        current_product = row["Product title"]
                    else:
                        current_product = row["Product title"]
                else:
                    if current_product:
                        df_old_merged.loc[idx, "Product title"] = current_product

            df_old_merged = df_old_merged[
                (df_old_merged["Product variant title"] != "ALL VARIANTS (TOTAL)") &
                (df_old_merged["Product variant title"] != "ALL PRODUCTS") &
                (df_old_merged["Delivery Rate"].notna()) & (df_old_merged["Delivery Rate"] != "")
            ]
            df_old_merged["Product title_norm"] = df_old_merged["Product title"].astype(str).str.strip().str.lower()
            df_old_merged["Product variant title_norm"] = df_old_merged["Product variant title"].astype(str).str.strip().str.lower()

            has_product_cost = "Product Cost (Input)" in df_old_merged.columns
            st.success(f"✅ Loaded {len(df_old_merged)} records with delivery rates from old merged data")
            if has_product_cost:
                product_cost_count = df_old_merged["Product Cost (Input)"].notna().sum()
                st.success(f"✅ Found {product_cost_count} records with product costs")

            preview_cols = ["Product title", "Product variant title", "Delivery Rate"]
            if has_product_cost:
                preview_cols.append("Product Cost (Input)")
            st.write(df_old_merged[preview_cols].head())
        else:
            st.warning("⚠️ Old merged file doesn't contain required columns: Product title, Product variant title, Delivery Rate")
            df_old_merged = None
    except Exception as e:
        st.error(f"❌ Error reading old merged file: {str(e)}")
        df_old_merged = None

# ---- CAMPAIGN DATA ----
if campaign_file:
    if campaign_file.name.endswith(".csv"):
        df_campaign = pd.read_csv(campaign_file)
    else:
        df_campaign = pd.read_excel(campaign_file)

    st.subheader("📂 Original Campaign Data")
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

    # ---- GROUP BY CANONICAL PRODUCT ----
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

    # ---- FINAL CAMPAIGN DATA STRUCTURE ----
    final_campaign_data = []
    has_purchases = "Purchases" in df_campaign.columns

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
            final_campaign_data.append(row)

    df_final_campaign = pd.DataFrame(final_campaign_data)

    if not df_final_campaign.empty:
        order = (
            df_final_campaign.groupby("Product")["Amount Spent (INR)"].sum().sort_values(ascending=False).index
        )
        df_final_campaign["Product"] = pd.Categorical(df_final_campaign["Product"], categories=order, ordered=True)
        df_final_campaign = df_final_campaign.sort_values("Product").reset_index(drop=True)
        df_final_campaign["Delivered Orders"] = ""
        df_final_campaign["Delivery Rate"] = ""

    st.subheader("🎯 Final Campaign Data Structure")
    st.write(df_final_campaign.drop(columns=["Product"], errors="ignore"))

    

# ---- SHOPIFY DATA ----
if shopify_file:
    if shopify_file.name.endswith(".csv"):
        df_shopify = pd.read_csv(shopify_file)
    else:
        df_shopify = pd.read_excel(shopify_file)

    required_cols = ["Product title", "Product variant title", "Product variant price", "Net items sold"]
    available_cols = [col for col in required_cols if col in df_shopify.columns]
    df_shopify = df_shopify[available_cols]

    # Add extra columns
    df_shopify["In Order"] = ""
    df_shopify["Product Cost (Input)"] = ""
    df_shopify["Delivery Rate"] = ""
    df_shopify["Delivered Orders"] = ""
    df_shopify["Net Revenue"] = ""
    df_shopify["Ad Spend (INR)"] = 0.0
    df_shopify["Shipping Cost"] = ""
    df_shopify["Operational Cost"] = ""
    df_shopify["Product Cost (Output)"] = ""
    df_shopify["Net Profit"] = ""
    df_shopify["Net Profit (%)"] = ""

    # ---- IMPORT DELIVERY RATES AND PRODUCT COSTS FROM OLD DATA ----
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
        
        st.success(f"✅ Successfully imported delivery rates for {delivery_matched_count} product variants from old data")
        if has_product_cost and product_cost_matched_count > 0:
            st.success(f"✅ Successfully imported product costs for {product_cost_matched_count} product variants from old data")
        elif has_product_cost:
            st.info("ℹ️ No product cost matches found in old data")

    # ---- STEP 3: CLEAN SHOPIFY PRODUCT TITLES TO MATCH CAMPAIGN ----
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

    # ---- ALLOCATE AD SPEND ----
    if grouped_campaign is not None:
        ad_spend_map = dict(zip(grouped_campaign["Product"], grouped_campaign["Total Amount Spent (INR)"]))

        for product, product_df in df_shopify.groupby("Canonical Product"):
            total_items = product_df["Net items sold"].sum()
            if total_items > 0 and product in ad_spend_map:
                total_spend_inr = ad_spend_map[product]
                ratio = product_df["Net items sold"] / total_items
                df_shopify.loc[product_df.index, "Ad Spend (INR)"] = total_spend_inr * ratio
    
    

    # ---- SORT PRODUCTS BY NET ITEMS SOLD (DESC) ----
    product_order = (
        df_shopify.groupby("Product title")["Net items sold"]
        .sum()
        .sort_values(ascending=False)
        .index
    )

    df_shopify["Product title"] = pd.Categorical(df_shopify["Product title"], categories=product_order, ordered=True)
    df_shopify = df_shopify.sort_values(by=["Product title"]).reset_index(drop=True)

    st.subheader("🛒 Shopify Data with Ad Spend (INR) & Extra Columns")
    
    # Show delivery rate and product cost import summary
    if df_old_merged is not None:
        delivery_rate_filled = df_shopify["Delivery Rate"].astype(str).str.strip()
        delivery_rate_filled = delivery_rate_filled[delivery_rate_filled != ""]
        
        product_cost_filled = df_shopify["Product Cost (Input)"].astype(str).str.strip()
        product_cost_filled = product_cost_filled[product_cost_filled != ""]
        
        st.info(f"📊 Delivery rates imported: {len(delivery_rate_filled)} out of {len(df_shopify)} variants")
        if len(product_cost_filled) > 0:
            st.info(f"📊 Product costs imported: {len(product_cost_filled)} out of {len(df_shopify)} variants")
    
    st.write(df_shopify)

# ✅ Build lookup of weighted avg price per product (only if Shopify data exists)
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

# ✅ Build lookup of weighted avg product cost per product
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

    # ✅ Build Shopify totals lookup for Delivered Orders & Delivery Rate
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



def convert_shopify_to_excel_staff(df):
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
        
        # Grand total format (different color)
        grand_total_format = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#EEEE0E", "font_name": "Calibri", "font_size": 11  # Green
        })
        
        # Product total format (different color)
        product_total_format = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#E7EE94", "font_name": "Calibri", "font_size": 11  # Yellow
        })
        
        variant_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "font_name": "Calibri", "font_size": 11
        })
        
        # Low items format for products with net items sold < 5
        low_items_product_format = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#DC4E23",  # Red
            "font_name": "Calibri", "font_size": 11
        })
        
        # Low items format for variants under low-performing products
        low_items_variant_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#FFCCCB",  # Light red
            "font_name": "Calibri", "font_size": 11
        })

        # Visible columns
        visible_cols = [
            "Product title", "Product variant title", "Product variant price",
            "Net items sold", "Ad Spend (USD)",
            "Product Cost (Input)", "Delivery Rate", "Score"
        ]

        # Write headers
        for col_num, col_name in enumerate(visible_cols):
            worksheet.write(0, col_num, col_name, header_format)

        # Insert GRAND TOTAL row right after header
        grand_total_row = 1
        worksheet.write(grand_total_row, 0, "GRAND TOTAL", grand_total_format)
        worksheet.write(grand_total_row, 1, "ALL PRODUCTS", grand_total_format)

        row = grand_total_row + 1
        product_total_rows = []

        for product, product_df in df.groupby("Product title"):
            product_total_row_idx = row
            product_total_rows.append(product_total_row_idx)

            # Calculate total net items sold for this product to determine if it's low performing
            total_net_items = product_df["Net items sold"].fillna(0).sum()
            is_low_performing = total_net_items < 5
            
            # Choose format based on performance
            current_product_format = low_items_product_format if is_low_performing else product_total_format
            current_variant_format = low_items_variant_format if is_low_performing else variant_format

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
                A_inr = variant.get("Ad Spend (INR)", 0.0) or 0.0
                A_usd = A_inr / 100
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

            # Totals formulas for product row
            excel_first = first_variant_row + 1
            excel_last = last_variant_row + 1
            worksheet.write_formula(product_total_row_idx, 3, f"=SUM(D{excel_first}:D{excel_last})", current_product_format)  # Net items sold
            worksheet.write_formula(product_total_row_idx, 4, f"=SUM(E{excel_first}:E{excel_last})", current_product_format)  # Ad Spend USD

            # Weighted avg Price
            worksheet.write_formula(product_total_row_idx, 2,
                f"=IF(SUM(D{excel_first}:D{excel_last})=0,0,"
                f"SUMPRODUCT(C{excel_first}:C{excel_last},D{excel_first}:D{excel_last})/SUM(D{excel_first}:D{excel_last}))",
                current_product_format)

            # Weighted avg Cost
            worksheet.write_formula(product_total_row_idx, 5,
                f"=IF(SUM(D{excel_first}:D{excel_last})=0,0,"
                f"SUMPRODUCT(F{excel_first}:F{excel_last},D{excel_first}:D{excel_last})/SUM(D{excel_first}:D{excel_last}))",
                current_product_format)

            # Weighted avg Delivery Rate
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
            
        # GRAND TOTAL formulas - FIXED to include all product total rows
        if product_total_rows:
            # Convert all product total row indices to Excel row numbers (1-based)
            excel_rows = [str(row_idx + 1) for row_idx in product_total_rows]
            rows_range = ",".join([f"D{row}" for row in excel_rows])
            
            # Net items sold - sum all product totals (no division by 2)
            worksheet.write_formula(grand_total_row, 3, f"=SUM({rows_range})", grand_total_format)
            
            # Ad Spend USD - sum all product totals (no division by 2)
            ad_spend_range = ",".join([f"E{row}" for row in excel_rows])
            worksheet.write_formula(grand_total_row, 4, f"=SUM({ad_spend_range})", grand_total_format)

            # For weighted averages, we need to use all individual variants, not product totals
            # Find the range of all variant rows (excluding product total rows)
            all_variant_rows = []
            current_row = grand_total_row + 1  # Start after grand total (row 2 in 0-indexed)
            
            for product, product_df in df.groupby("Product title"):
                current_row += 1  # Skip product total row
                for _ in range(len(product_df)):  # Add variant rows
                    all_variant_rows.append(current_row)
                    current_row += 1
            
            if all_variant_rows:
                first_variant_excel = all_variant_rows[0]
                last_variant_excel = all_variant_rows[-1]
                
                # Weighted avg Price
                worksheet.write_formula(grand_total_row, 2,
                    f"=IF(SUM(D{first_variant_excel}:D{last_variant_excel})=0,0,"
                    f"SUMPRODUCT(C{first_variant_excel}:C{last_variant_excel},D{first_variant_excel}:D{last_variant_excel})/SUM(D{first_variant_excel}:D{last_variant_excel}))",
                    grand_total_format)

                # Weighted avg Cost
                worksheet.write_formula(grand_total_row, 5,
                    f"=IF(SUM(D{first_variant_excel}:D{last_variant_excel})=0,0,"
                    f"SUMPRODUCT(F{first_variant_excel}:F{last_variant_excel},D{first_variant_excel}:D{last_variant_excel})/SUM(D{first_variant_excel}:D{last_variant_excel}))",
                    grand_total_format)

                # Weighted avg Delivery Rate
                worksheet.write_formula(grand_total_row, 6,
                    f"=IF(SUM(D{first_variant_excel}:D{last_variant_excel})=0,0,"
                    f"SUMPRODUCT(G{first_variant_excel}:G{last_variant_excel},D{first_variant_excel}:D{last_variant_excel})/SUM(D{first_variant_excel}:D{last_variant_excel}))",
                    grand_total_format)

            # Score for grand total
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
    


# ---- SHOPIFY DOWNLOAD ----
if df_shopify is not None:
    export_df = df_shopify.drop(columns=["Product Name", "Canonical Product"], errors="ignore")

    shopify_excel = convert_shopify_to_excel_staff(export_df)
    st.download_button(
        label="📥 Download Processed Shopify File (Excel)",
        data=shopify_excel,
        file_name="processed_shopify.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
else:
    st.warning("⚠️ Please upload a Shopify file to process.")



def safe_write(ws, row, col, value, fmt=None):
    """Safe cell writer that avoids crashes on None or bad types."""
    if value is None:
        value = ""
    try:
        if fmt:
            ws.write(row, col, value, fmt)
        else:
            ws.write(row, col, value)
    except Exception:
        ws.write(row, col, str(value))



def convert_final_campaign_to_excel(df, unmatched_campaigns):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        workbook = writer.book
        ws = workbook.add_worksheet("Final Campaign")
        writer.sheets["Final Campaign"] = ws

        # === FORMATS ===
        header_fmt = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#BDD7EE", "font_name": "Calibri", "font_size": 11
        })
        product_total_fmt = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#FFD966", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        campaign_fmt = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })

        # === ONLY SHOW THESE COLUMNS TO STAFF ===
        columns = [
            "Product", "Campaign Name", "Amount Spent (USD)",
            "Purchases", "Cost Per Purchase (USD)", "Score"
        ]

        # Write header
        for c, col_name in enumerate(columns):
            ws.write(0, c, col_name, header_fmt)

        # Track campaigns that have Shopify data vs those that don't
        matched_campaigns = []
        staff_unmatched_campaigns = []
        
        for product, product_df in df.groupby("Product"):
            has_shopify_data = (product in shopify_totals or 
                                product in avg_price_lookup or 
                                product in avg_product_cost_lookup)
            
            for _, campaign_row in product_df.iterrows():
                campaign_info = {
                    'Product': str(product) if pd.notna(product) else '',
                    'Campaign Name': str(campaign_row.get("Campaign Name", '')) if pd.notna(campaign_row.get("Campaign Name", '')) else '',
                    'Amount Spent (USD)': round(float(campaign_row.get("Amount Spent (USD)", 0)), 2) if pd.notna(campaign_row.get("Amount Spent (USD)", 0)) else 0.0,
                    'Purchases': int(campaign_row.get("Purchases", 0)) if pd.notna(campaign_row.get("Purchases", 0)) else 0,
                    'Has Shopify Data': has_shopify_data
                }
                if has_shopify_data:
                    matched_campaigns.append(campaign_info)
                else:
                    staff_unmatched_campaigns.append(campaign_info)

        # === COL INDEX MAP (visible only) ===
        spent_col = columns.index("Amount Spent (USD)")
        purchases_col = columns.index("Purchases")
        cpp_col = columns.index("Cost Per Purchase (USD)")
        score_col = columns.index("Score")

        row = 1
        for product, product_df in df.groupby("Product"):
            avg_price = avg_price_lookup.get(product, 0)
            product_cost = avg_product_cost_lookup.get(product, 0)
            delivery_rate = shopify_totals.get(product, {}).get("Delivery Rate", 0)

            # Pre-compute CPP for sorting
            product_df = product_df.copy()
            product_df["CPP"] = product_df.apply(
                lambda r: round(r["Amount Spent (USD)"] / r["Purchases"], 2) if pd.notna(r.get("Purchases")) and r.get("Purchases", 0) > 0 else float("inf"),
                axis=1
            )
            product_df = product_df.sort_values("CPP", ascending=True)

            # --- Product total row ---
            product_row = row
            safe_write(ws, product_row, 0, product, product_total_fmt)
            safe_write(ws, product_row, 1, "ALL CAMPAIGNS (TOTAL)", product_total_fmt)

            # Totals (formulas)
            purchases_refs = [f"{xl_col_to_name(purchases_col)}{i+1}" for i in range(row+1, row+1+len(product_df)+1)]
            spent_refs = [f"{xl_col_to_name(spent_col)}{i+1}" for i in range(row+1, row+1+len(product_df)+1)]

            if purchases_refs:
                ws.write_formula(product_row, purchases_col, f"=ROUND(SUM({','.join(purchases_refs)}),2)", product_total_fmt)
            if spent_refs:
                ws.write_formula(product_row, spent_col, f"=ROUND(SUM({','.join(spent_refs)}),2)", product_total_fmt)

            # Cost Per Purchase (safe divide)
            spent_ref = xl_col_to_name(spent_col) + str(product_row+1)
            purchases_ref = xl_col_to_name(purchases_col) + str(product_row+1)
            cpp_formula = f"=IF(N({purchases_ref})>0,ROUND(N({spent_ref})/N({purchases_ref}),2),0)"
            ws.write_formula(product_row, cpp_col, cpp_formula, product_total_fmt)

            # Score formula
            score_formula = (
                f"=IF(AND(N({purchases_ref})>0,{avg_price}>0),"
                f"ROUND((({avg_price}*N({purchases_ref})*{delivery_rate})"
                f"- (N({spent_ref})*100)"
                f"- (77*N({purchases_ref}))"
                f"- (65*N({purchases_ref}))"
                f"- ({product_cost}*N({purchases_ref})*{delivery_rate}))"
                f"/(({avg_price}*N({purchases_ref})*{delivery_rate})*0.1),2),0)"
            )
            ws.write_formula(product_row, score_col, score_formula, product_total_fmt)

            # --- Campaign rows (sorted by CPP) ---
            row += 1
            for _, campaign in product_df.iterrows():
                campaign_row = row
                excel_row = campaign_row + 1

                safe_write(ws, campaign_row, 0, "", campaign_fmt)
                safe_write(ws, campaign_row, 1, campaign.get("Campaign Name", ""), campaign_fmt)
                safe_write(ws, campaign_row, spent_col, round(campaign.get("Amount Spent (USD)", 0), 2), campaign_fmt)
                safe_write(ws, campaign_row, purchases_col, campaign.get("Purchases", 0), campaign_fmt)

                spent_ref = xl_col_to_name(spent_col) + str(excel_row)
                purchases_ref = xl_col_to_name(purchases_col) + str(excel_row)

                cpp_formula = f"=IF(N({purchases_ref})>0,ROUND(N({spent_ref})/N({purchases_ref}),2),0)"
                ws.write_formula(campaign_row, cpp_col, cpp_formula, campaign_fmt)

                score_formula = (
                    f"=IF(AND(N({purchases_ref})>0,{avg_price}>0),"
                    f"ROUND((({avg_price}*N({purchases_ref})*{delivery_rate})"
                    f"- (N({spent_ref})*100)"
                    f"- (77*N({purchases_ref}))"
                    f"- (65*N({purchases_ref}))"
                    f"- ({product_cost}*N({purchases_ref})*{delivery_rate}))"
                    f"/(({avg_price}*N({purchases_ref})*{delivery_rate})*0.1),2),0)"
                )
                ws.write_formula(campaign_row, score_col, score_formula, campaign_fmt)

                row += 1

            row += 1  # space after product group

        # === UNMATCHED CAMPAIGNS SHEET ===
        unmatched_sheet = workbook.add_worksheet("Unmatched Campaigns")
        writer.sheets["Unmatched Campaigns"] = unmatched_sheet

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

        unmatched_headers = ["Status", "Product", "Campaign Name", "Amount Spent (USD)", 
                           "Purchases", "Cost Per Purchase (USD)", "Reason"]
        
        for col_num, header in enumerate(unmatched_headers):
            safe_write(unmatched_sheet, 0, col_num, header, unmatched_header_format)

        summary_row = 1
        safe_write(unmatched_sheet, summary_row, 0, "SUMMARY", unmatched_header_format)
        safe_write(unmatched_sheet, summary_row + 1, 0, f"Total Campaigns: {len(matched_campaigns) + len(staff_unmatched_campaigns)}", matched_summary_format)
        safe_write(unmatched_sheet, summary_row + 2, 0, f"Matched with Shopify: {len(matched_campaigns)}", matched_summary_format)
        safe_write(unmatched_sheet, summary_row + 3, 0, f"Unmatched with Shopify: {len(staff_unmatched_campaigns)}", unmatched_data_format)

        current_row = summary_row + 5
        if staff_unmatched_campaigns:
            safe_write(unmatched_sheet, current_row, 0, "CAMPAIGNS WITHOUT SHOPIFY DATA", unmatched_header_format)
            current_row += 1
            for campaign in staff_unmatched_campaigns:
                cpp = round(campaign["Amount Spent (USD)"] / campaign["Purchases"], 2) if campaign["Purchases"] > 0 else 0
                safe_write(unmatched_sheet, current_row, 0, "UNMATCHED", unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 1, campaign["Product"], unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 2, campaign["Campaign Name"], unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 3, campaign["Amount Spent (USD)"], unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 4, campaign["Purchases"], unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 5, cpp, unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 6, "No matching Shopify product found", unmatched_data_format)
                current_row += 1

        if matched_campaigns:
            current_row += 2
            safe_write(unmatched_sheet, current_row, 0, "CAMPAIGNS WITH SHOPIFY DATA (FOR REFERENCE)", unmatched_header_format)
            current_row += 1
            display_count = min(10, len(matched_campaigns))
            for i in range(display_count):
                campaign = matched_campaigns[i]
                cpp = round(campaign["Amount Spent (USD)"] / campaign["Purchases"], 2) if campaign["Purchases"] > 0 else 0
                safe_write(unmatched_sheet, current_row, 0, "MATCHED", matched_summary_format)
                safe_write(unmatched_sheet, current_row, 1, campaign["Product"], matched_summary_format)
                safe_write(unmatched_sheet, current_row, 2, campaign["Campaign Name"], matched_summary_format)
                safe_write(unmatched_sheet, current_row, 3, campaign["Amount Spent (USD)"], matched_summary_format)
                safe_write(unmatched_sheet, current_row, 4, campaign["Purchases"], matched_summary_format)
                safe_write(unmatched_sheet, current_row, 5, cpp, matched_summary_format)
                safe_write(unmatched_sheet, current_row, 6, "Successfully matched with Shopify", matched_summary_format)
                current_row += 1

            if len(matched_campaigns) > 10:
                safe_write(unmatched_sheet, current_row, 0, f"... and {len(matched_campaigns) - 10} more matched campaigns", matched_summary_format)

        unmatched_sheet.set_column(0, 0, 12)
        unmatched_sheet.set_column(1, 1, 25)
        unmatched_sheet.set_column(2, 2, 35)
        unmatched_sheet.set_column(3, 3, 18)
        unmatched_sheet.set_column(4, 4, 12)
        unmatched_sheet.set_column(5, 5, 20)
        unmatched_sheet.set_column(6, 6, 35)

        ws.freeze_panes(1, 0)
        for i, col in enumerate(columns):
            if col in ("Product", "Campaign Name"):
                ws.set_column(i, i, 35)
            else:
                ws.set_column(i, i, 18)

    return output.getvalue()



# ---- CAMPAIGN DOWNLOAD ----
if campaign_file:
    def convert_df_to_excel(df):
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Processed Data")
        return output.getvalue()

    # Download processed campaign data (simple format)
    excel_data = convert_df_to_excel(grouped_campaign)
    st.download_button(
        label="📥 Download Processed Campaign File (Excel)",
        data=excel_data,
        file_name="processed_campaigns.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    # Download final campaign data (structured format like Shopify)
    if 'df_final_campaign' in locals() and not df_final_campaign.empty:
        final_campaign_excel = convert_final_campaign_to_excel(df_final_campaign,[])
        if final_campaign_excel:
            st.download_button(
                label="🎯 Download Final Campaign File (Structured Excel)",
                data=final_campaign_excel,
                file_name="final_campaign_data.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
