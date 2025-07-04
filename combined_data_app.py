import pandas as pd
import numpy as np
import os
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from datetime import datetime
import time
import platform
import threading
import re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

class AutomatedDataProcessor:
    def __init__(self):
        # Check SharePoint access first
        if not self.check_sharepoint_access():
            self.show_access_denied()
            return
            
        self.root = tk.Tk()
        self.root.title("Automated Data Processor")
        self.root.geometry("700x600")
        self.root.configure(bg='white')
        self.root.resizable(False, False)
        
        # Core data
        self.production_files = []
        self.consolidated_data = pd.DataFrame()
        self.project_tracker_data = pd.DataFrame()
        self.combined_data = pd.DataFrame()
        self.final_output_data = pd.DataFrame()
        
        # Paths
        self.project_tracker_path = ""
        self.processing_logs = []  # Store logs in background (not shown in UI)
        self.selected_start_date = None
        self.selected_end_date = None
        self.date_filter_applied = False
        
        # Setup paths
        self.setup_paths()
        
        # Target columns for Production Item Lists
        self.target_columns = ['Item Number', 'Product Vendor Company Name', 'Brand', 'Product Name', 'SKU New/Existing']
        
        # Final output column order with renamed headers
        self.final_columns = [
            'HUGO ID', 'Product Vendor Company Name', 'Item Number', 'Product Name', 'Brand', 'SKU', 
            'Artwork Release Date', '5 Weeks After Artwork Release', 'Entered into HUGO Date', 
            'Entered in HUGO?', 'Store Date', 'Re-Release Status', 'Packaging Format 1', 
            'Printer Company Name 1', 'Vendor e-mail 1', 'Printer e-mail 1', 
            'Printer Code 1 (LW Code)', 'File Name'
        ]
        
        # Setup GUI
        self.setup_gui()
        self.center_window()
    
    def check_sharepoint_access(self):
        """Check if user has access to SharePoint directories"""
        try:
            is_mac = platform.system() == 'Darwin'
            
            if is_mac:
                base_path = os.path.expanduser("~/Lowe's Companies Inc")
            else:
                base_path = "C:\\Users\\mjayash\\Lowe's Companies Inc"
            
            # Check if any SharePoint path exists
            sharepoint_paths = [
                os.path.join(base_path, "Private Brands - Packaging Operations - Building Products"),
                os.path.join(base_path, "Private Brands - Packaging Operations - Hardlines & Seasonal"),
                os.path.join(base_path, "Private Brands - Packaging Operations - Home Décor")
            ]
            
            for path in sharepoint_paths:
                if os.path.exists(path):
                    return True
                    
            return False
            
        except Exception:
            return False
    
    def show_access_denied(self):
        """Show access denied message and exit"""
        root = tk.Tk()
        root.withdraw()
        
        messagebox.showerror(
            "Access Denied", 
            "SharePoint Access Required\n\n"
            "This application requires access to Lowe's SharePoint directories.\n"
            "Please ensure you have proper network access and try again.\n\n"
            "Contact IT support if you need SharePoint access."
        )
        
        root.destroy()
        return
    
    def setup_paths(self):
        """Setup paths"""
        self.is_mac = platform.system() == 'Darwin'
        
        if self.is_mac:
            base_path = os.path.expanduser("~/Lowe's Companies Inc")
        else:
            base_path = "C:\\Users\\mjayash\\Lowe's Companies Inc"
        
        # SharePoint paths
        self.sharepoint_paths = [
            os.path.join(base_path, "Private Brands - Packaging Operations - Building Products"),
            os.path.join(base_path, "Private Brands - Packaging Operations - Hardlines & Seasonal"),
            os.path.join(base_path, "Private Brands - Packaging Operations - Home Décor")
        ]
        
        # Default project tracker path
        self.default_project_tracker_path = os.path.join(base_path, "Private Brands Packaging File Transfer - PQM Compliance reporting", "Project tracker.xlsx")
        
        # Output folder
        desktop = os.path.expanduser("~/Desktop") if self.is_mac else os.path.join(os.path.expanduser("~"), "Desktop")
        self.output_folder = os.path.join(desktop, "Automated_Data_Processing_Output")
        os.makedirs(self.output_folder, exist_ok=True)
    
    def center_window(self):
        """Center the window on screen"""
        self.root.update_idletasks()
        width = self.root.winfo_width()
        height = self.root.winfo_height()
        pos_x = (self.root.winfo_screenwidth() // 2) - (width // 2)
        pos_y = (self.root.winfo_screenheight() // 2) - (height // 2)
        self.root.geometry(f'{width}x{height}+{pos_x}+{pos_y}')
    
    def log_message(self, message):
        """Store log messages in background (not displayed in UI)"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted_message = f"[{timestamp}] {message}"
        self.processing_logs.append(formatted_message)
    
    # ========== AUTOMATED WORKFLOW METHODS ==========
    
    def run_automated_workflow(self, start_date, end_date):
        """Run complete automated workflow"""
        def process_thread():
            try:
                total_start = time.time()
                self.log_message("Starting automated workflow...")
                
                # Update status
                self.update_status("Processing... Please wait (this may take several minutes)")
                self.progress_bar.start()
                
                # Step 1: Scan production folders
                if not self.scan_production_folders():
                    raise Exception("No production files found")
                
                # Step 2: Extract production data
                if not self.intelligent_data_extraction():
                    raise Exception("Production data extraction failed")
                
                # Step 3: Process project tracker
                if not self.process_project_tracker():
                    raise Exception("Project tracker processing failed")
                
                # Step 4: Combine datasets
                if not self.combine_datasets():
                    raise Exception("Data combination failed")
                
                # Step 5: Filter by date range
                if not self.filter_by_date_range(start_date, end_date):
                    raise Exception("Date filtering failed or no records in range")
                
                # Step 6: Format final output
                if not self.format_final_output():
                    raise Exception("Final output formatting failed")
                
                # Step 7: Save all outputs
                output_files = self.save_all_outputs(start_date, end_date)
                
                total_time = time.time() - total_start
                
                # Stop progress and show success
                self.progress_bar.stop()
                self.update_status("Processing completed successfully!")
                
                # Show success message
                final_records = len(self.final_output_data)
                success_msg = (f"Processing Completed Successfully!\n\n"
                              f"Total Time: {total_time:.1f} seconds\n"
                              f"Date Range: {start_date} to {end_date}\n"
                              f"Final Records: {final_records:,}\n"
                              f"Output Columns: {len(self.final_columns)}\n"
                              f"Files Created: {len(output_files)}\n\n"
                              f"All files saved to Desktop → Automated_Data_Processing_Output")
                
                messagebox.showinfo("Success!", success_msg)
                
                # Enable output folder button
                self.open_folder_btn.config(state='normal')
                
            except Exception as e:
                self.progress_bar.stop()
                self.update_status("Processing failed. Please check your data and try again.")
                self.log_message(f"Error: {str(e)}")
                messagebox.showerror("Error", f"Processing failed: {str(e)}")
        
        # Start processing in background thread
        threading.Thread(target=process_thread, daemon=True).start()
    
    # ========== DATA PROCESSING METHODS ==========
    
    def scan_production_folders(self):
        """Scan for production item list folders"""
        self.log_message("Scanning production folders...")
        
        all_files = []
        
        for sp_path in self.sharepoint_paths:
            if not os.path.exists(sp_path):
                continue
                
            try:
                for root, dirs, files in os.walk(sp_path):
                    if root.endswith("_Production Item List"):
                        excel_files = [f for f in files 
                                     if f.lower().endswith(('.xlsx', '.xls', '.xlsm')) 
                                     and not f.startswith(('~', '.', '$'))]
                        
                        for excel_file in excel_files:
                            full_path = os.path.join(root, excel_file)
                            all_files.append(full_path)
            except Exception as e:
                self.log_message(f"Error scanning {sp_path}: {str(e)}")
        
        self.production_files = all_files
        self.log_message(f"Found {len(all_files)} production files")
        return len(all_files) > 0
    
    def intelligent_data_extraction(self):
        """Extract data with intelligent header detection"""
        self.log_message("Extracting production data...")
        
        # Updated column patterns with new names
        column_patterns = {
            'Item Number': ['item #', 'item#', 'itemnumber', 'item number', 'item no', 'itemno'],
            'Product Vendor Company Name': ['vendor name', 'vendorname', 'vendor', 'supplier'],
            'Brand': ['brand', 'brandname', 'brand name'],
            'Product Name': ['item description', 'itemdescription', 'description', 'product description', 'desc', 'product name'],
            'SKU New/Existing': ['SKU', 'SKU new/existing', 'SKU new existing', 'SKU new/carry forward', 'SKU new carry forward', 'SKU new']
        }
        
        def extract_from_file(file_path):
            try:
                df = pd.read_excel(file_path, header=None, nrows=1000)
                if df.empty:
                    return pd.DataFrame()
                
                best_extraction = pd.DataFrame()
                best_score = 0
                
                for potential_header_row in range(min(50, len(df))):
                    try:
                        potential_headers = df.iloc[potential_header_row].astype(str).str.lower().str.strip()
                        
                        # Handle multi-line headers
                        combined_headers = potential_headers.copy()
                        if potential_header_row + 1 < len(df):
                            next_row_headers = df.iloc[potential_header_row + 1].astype(str).str.lower().str.strip()
                            combined_headers = potential_headers + " " + next_row_headers
                            combined_headers = combined_headers.str.replace(r'\s+', ' ', regex=True).str.strip()
                        
                        column_mapping = {}
                        score = 0
                        
                        for target_col, search_patterns in column_patterns.items():
                            for col_idx, header in enumerate(combined_headers):
                                if pd.isna(header) or header == '' or header == 'nan' or 'nan nan' in header:
                                    continue
                                
                                clean_header = re.sub(r'[^a-z0-9]', '', header.strip().lower())
                                
                                for pattern in search_patterns:
                                    clean_pattern = re.sub(r'[^a-z0-9]', '', pattern.lower())
                                    if clean_pattern in clean_header:
                                        column_mapping[target_col] = col_idx
                                        score += 1
                                        break
                                
                                if target_col in column_mapping:
                                    break
                        
                        if score >= 2:
                            try:
                                full_df = pd.read_excel(file_path, header=potential_header_row, nrows=10000)
                                
                                if not full_df.empty and len(full_df.columns) > max(column_mapping.values()):
                                    extracted_data = pd.DataFrame()
                                    
                                    for target_col in self.target_columns:
                                        if target_col in column_mapping:
                                            col_idx = column_mapping[target_col]
                                            if col_idx < len(full_df.columns):
                                                source_col_name = full_df.columns[col_idx]
                                                extracted_data[target_col] = full_df[source_col_name].astype(str).str.strip()
                                        else:
                                            extracted_data[target_col] = ''
                                    
                                    # Clean Item Number and ensure it's never empty
                                    if 'Item Number' in extracted_data.columns:
                                        def clean_item_number(value):
                                            try:
                                                if pd.isna(value) or str(value).strip() == '' or str(value).lower() in ['nan', 'none']:
                                                    return ''
                                                clean_val = re.sub(r'[^\d]', '', str(value))
                                                if clean_val and clean_val.isdigit():
                                                    return int(clean_val)
                                                return ''
                                            except:
                                                return ''
                                        
                                        extracted_data['Item Number'] = extracted_data['Item Number'].apply(clean_item_number)
                                        
                                        # CRITICAL: Only keep rows with valid Item Number (never empty)
                                        extracted_data = extracted_data[
                                            (extracted_data['Item Number'] != '') & 
                                            (extracted_data['Item Number'] != 0)
                                        ]
                                        
                                        extracted_data['Item Number'] = extracted_data['Item Number'].apply(
                                            lambda x: str(int(x)) if x != '' and x != 0 else ''
                                        )
                                    
                                    # Only keep rows with valid Item Number
                                    if 'Item Number' in extracted_data.columns:
                                        valid_items = extracted_data['Item Number'] != ''
                                        extracted_data = extracted_data[valid_items]
                                    
                                    if len(extracted_data) > 0:
                                        file_name = os.path.basename(file_path)
                                        extracted_data['Source_File'] = file_name
                                        extracted_data['Source_Folder'] = os.path.basename(os.path.dirname(file_path))
                                        
                                        if score > best_score or len(extracted_data) > len(best_extraction):
                                            best_extraction = extracted_data.copy()
                                            best_score = score
                            
                            except Exception:
                                continue
                    
                    except Exception:
                        continue
                
                return best_extraction
                
            except Exception:
                return pd.DataFrame()
        
        # Process files in parallel
        all_extracted_data = []
        
        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = [executor.submit(extract_from_file, file_path) for file_path in self.production_files]
            
            for future in as_completed(futures):
                result = future.result()
                if not result.empty:
                    all_extracted_data.append(result)
        
        # Consolidate data
        if all_extracted_data:
            self.consolidated_data = pd.concat(all_extracted_data, ignore_index=True)
            self.consolidated_data = self.consolidated_data.drop_duplicates(subset=['Item Number', 'Source_File'], keep='first')
            
            # Final check: ensure Item Number is never empty
            self.consolidated_data = self.consolidated_data[self.consolidated_data['Item Number'] != '']
            
            self.log_message(f"Extracted {len(self.consolidated_data)} records with valid Item Numbers")
            return True
        else:
            self.log_message("No data extracted")
            return False
    
    def process_project_tracker(self):
        """Process project tracker file"""
        try:
            if not self.project_tracker_path or not os.path.exists(self.project_tracker_path):
                return False
            
            self.log_message("Processing project tracker...")
            
            df = pd.read_excel(self.project_tracker_path)
            
            def find_column(df, possible_names):
                df_cols_lower = [col.lower() for col in df.columns]
                for name in possible_names:
                    name_lower = name.lower()
                    for i, col in enumerate(df_cols_lower):
                        if name_lower in col or col in name_lower:
                            return df.columns[i]
                return None
            
            # Column mappings
            column_mappings = {
                'HUGO ID': ['PKG3'],
                'File Name': ['File Name', 'FileName', 'Name'],
                'Rounds': ['Rounds', 'Round'],
                'Printer Company Name 1': ['PAComments', 'PA Comments', 'Comments'],
                'Vendor e-mail 1': ['VendorEmail', 'Vendor Email', 'VendorE-mail'],
                'Printer e-mail 1': ['PrinterEmail', 'Printer Email', 'PrinterE-mail'],
                'PKG1': ['PKG1'],
                'Artwork Release Date': ['ReleaseDate', 'Release Date'],
                '5 Weeks After Artwork Release': ['5 Weeks After Artwork Release', '5 weeks after artwork release'],
                'Entered into HUGO Date': ['entered into HUGO Date', 'Entered into HUGO Date'],
                'Entered in HUGO?': ['Entered in HUGO?', 'entered in HUGO?'],
                'Store Date': ['Store Date', 'store date'],
                'Packaging Format 1': ['Packaging Format 1', 'packaging format 1'],
                'Printer Code 1 (LW Code)': ['Printer Code 1 (LW Code)', 'printer code 1 (LW Code)']
            }
            
            # Find columns
            found_columns = {}
            for target_name, possible_names in column_mappings.items():
                found_col = find_column(df, possible_names)
                if found_col:
                    found_columns[target_name] = found_col
            
            if 'Rounds' not in found_columns:
                return False
            
            # Filter data
            rounds_col = found_columns['Rounds']
            filter_values = ["File Release", "File Re-Release R2", "File Re-Release R3"]
            mask = df[rounds_col].isin(filter_values)
            filtered_df = df[mask].copy()
            
            if len(filtered_df) == 0:
                return False
            
            # Create result dataframe
            result = pd.DataFrame(index=filtered_df.index)
            
            # Map all columns
            for target_name, source_col in found_columns.items():
                if target_name == 'Artwork Release Date':
                    # Special date formatting
                    release_dates = filtered_df[source_col]
                    date_mask = pd.notna(release_dates) & (release_dates != "")
                    result[target_name] = ""
                    if date_mask.any():
                        valid_dates = pd.to_datetime(release_dates[date_mask], errors='coerce')
                        formatted_dates = valid_dates.dt.strftime("%d/%m/%y")
                        result.loc[date_mask, target_name] = formatted_dates
                else:
                    result[target_name] = filtered_df[source_col].fillna("")
            
            # Calculate Re-Release Status with empty cells for "No"
            rounds_upper = filtered_df[found_columns['Rounds']].astype(str).str.upper()
            re_release_status = np.where(
                rounds_upper.str.contains('R2|R3', na=False, regex=True), 
                'Yes', 
                ''  # EMPTY instead of "No" as requested
            )
            result['Re-Release Status'] = re_release_status
            
            self.project_tracker_data = result
            self.log_message(f"Processed {len(result)} project tracker records")
            return True
            
        except Exception as e:
            self.log_message(f"Project tracker error: {str(e)}")
            return False
    
    def combine_datasets(self):
        """Combine datasets with enhanced number cleaning"""
        try:
            self.log_message("Combining datasets...")
            
            if self.consolidated_data.empty or self.project_tracker_data.empty:
                return False
            
            step1_data = self.consolidated_data.copy()
            step2_data = self.project_tracker_data.copy()
            
            # Enhanced number cleaning
            def clean_to_number(value):
                try:
                    if pd.isna(value) or str(value).strip() == '' or str(value).lower() in ['nan', 'none', 'null']:
                        return ''
                    
                    clean_val = str(value).strip()
                    
                    # Handle Excel scientific notation
                    if 'e+' in clean_val.lower() or 'e-' in clean_val.lower():
                        try:
                            float_val = float(clean_val)
                            clean_val = f"{float_val:.0f}"
                        except:
                            pass
                    
                    # Remove decimal points
                    if '.' in clean_val:
                        clean_val = clean_val.split('.')[0]
                    
                    # Remove non-digits
                    numbers_only = re.sub(r'[^\d]', '', clean_val)
                    
                    if numbers_only and numbers_only.isdigit():
                        return str(int(numbers_only))
                    
                    return ''
                except:
                    return ''
            
            # Clean merge keys
            step1_data['Merge_Key'] = step1_data['Item Number'].apply(clean_to_number)
            step2_data['Merge_Key'] = step2_data['PKG1'].apply(clean_to_number)
            
            # Remove empty keys and duplicates
            step1_valid = step1_data[step1_data['Merge_Key'] != ''].copy()
            step2_valid = step2_data[step2_data['Merge_Key'] != ''].copy()
            
            step1_valid = step1_valid.drop_duplicates(subset=['Merge_Key'], keep='first')
            step2_valid = step2_valid.drop_duplicates(subset=['Merge_Key'], keep='first')
            
            # Merge datasets
            combined = pd.merge(step1_valid, step2_valid, on='Merge_Key', how='outer', indicator=True)
            
            # Add data source indicators
            combined['Data_Source'] = combined['_merge'].map({
                'both': 'Step1 + Step2',
                'left_only': 'Step1 Only',
                'right_only': 'Step2 Only'
            })
            
            if '_merge' in combined.columns:
                combined = combined.drop(columns=['_merge'])
            
            self.combined_data = combined
            
            matched_count = len(combined[combined['Data_Source'] == 'Step1 + Step2'])
            self.log_message(f"Combined datasets: {len(combined)} total, {matched_count} matched")
            return True
            
        except Exception as e:
            self.log_message(f"Combination error: {str(e)}")
            return False
    
    def filter_by_date_range(self, start_date, end_date):
        """Filter by date range"""
        try:
            self.log_message(f"Filtering by date range: {start_date} to {end_date}")
            
            if self.combined_data.empty:
                return False
            
            # Find date column
            date_column = None
            for col in self.combined_data.columns:
                if 'artwork release date' in col.lower():
                    date_column = col
                    break
            
            if not date_column:
                self.log_message("Artwork Release Date column not found")
                return False
            
            filtered_df = self.combined_data.copy()
            
            def parse_date_safe(date_val):
                try:
                    if pd.isna(date_val) or str(date_val).strip() == '' or str(date_val).lower() in ['nan', 'none', 'nat']:
                        return None
                    parsed = pd.to_datetime(date_val, errors='coerce')
                    return parsed.date() if pd.notna(parsed) else None
                except:
                    return None
            
            filtered_df['Parsed_Date'] = filtered_df[date_column].apply(parse_date_safe)
            
            # Apply date filter
            mask = (
                filtered_df['Parsed_Date'].notna() & 
                (filtered_df['Parsed_Date'] >= start_date) & 
                (filtered_df['Parsed_Date'] <= end_date)
            )
            
            filtered_df = filtered_df[mask].copy()
            
            # Remove temporary column
            if 'Parsed_Date' in filtered_df.columns:
                filtered_df = filtered_df.drop(columns=['Parsed_Date'])
            
            self.combined_data = filtered_df
            
            self.log_message(f"Date filtering complete: {len(filtered_df)} records")
            return len(filtered_df) > 0
            
        except Exception as e:
            self.log_message(f"Date filtering error: {str(e)}")
            return False
    
    def format_final_output(self):
        """Format final output with renamed columns"""
        try:
            self.log_message("Formatting final output...")
            
            if self.combined_data.empty:
                return False
            
            # Create final output dataframe
            final_df = pd.DataFrame()
            
            # Column mapping from combined data to final output (with renamed columns)
            column_mapping = {
                'HUGO ID': 'HUGO ID',
                'Product Vendor Company Name': 'Product Vendor Company Name',  # Renamed from Vendor Name
                'Item Number': 'Item Number',  # Renamed from Item #
                'Product Name': 'Product Name',  # Renamed from Item Description
                'Brand': 'Brand',
                'SKU': 'SKU New/Existing',  # Renamed
                'Artwork Release Date': 'Artwork Release Date',
                '5 Weeks After Artwork Release': '5 Weeks After Artwork Release',
                'Entered into HUGO Date': 'Entered into HUGO Date',
                'Entered in HUGO?': 'Entered in HUGO?',
                'Store Date': 'Store Date',
                'Re-Release Status': 'Re-Release Status',
                'Packaging Format 1': 'Packaging Format 1',
                'Printer Company Name 1': 'Printer Company Name 1',
                'Vendor e-mail 1': 'Vendor e-mail 1',
                'Printer e-mail 1': 'Printer e-mail 1',
                'Printer Code 1 (LW Code)': 'Printer Code 1 (LW Code)',
                'File Name': 'File Name'
            }
            
            # Extract columns in exact order
            for final_col in self.final_columns:
                if final_col in column_mapping:
                    source_col = column_mapping[final_col]
                    if source_col in self.combined_data.columns:
                        final_df[final_col] = self.combined_data[source_col]
                    else:
                        final_df[final_col] = ''
                else:
                    final_df[final_col] = ''
            
            # Clean up the data
            final_df = final_df.fillna('')
            
            # CRITICAL: Only keep records with valid Item Number (never empty)
            valid_mask = (final_df['Item Number'].astype(str).str.strip() != '') & (final_df['Item Number'].astype(str).str.strip() != 'nan')
            final_df = final_df[valid_mask]
            
            self.final_output_data = final_df
            
            self.log_message(f"Final formatting complete: {len(final_df)} records")
            return True
            
        except Exception as e:
            self.log_message(f"Formatting error: {str(e)}")
            return False
    
    def save_all_outputs(self, start_date, end_date):
        """Save all output files"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            date_range_str = f"{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}"
            
            output_files = []
            
            # Save final formatted output (main file)
            if not self.final_output_data.empty:
                final_file = os.path.join(self.output_folder, f"Final_Output_{date_range_str}_{timestamp}.xlsx")
                
                with pd.ExcelWriter(final_file, engine='xlsxwriter') as writer:
                    # Main data sheet
                    self.final_output_data.to_excel(writer, sheet_name='Final Data', index=False)
                    
                    # Summary sheet
                    summary_data = {
                        'Metric': [
                            'Total Final Records',
                            'Date Range Start',
                            'Date Range End',
                            'Total Columns',
                            'Processing Date',
                            'Project Tracker File',
                            'Records with Item Number',
                            'Records with HUGO ID'
                        ],
                        'Value': [
                            len(self.final_output_data),
                            start_date.strftime('%Y-%m-%d'),
                            end_date.strftime('%Y-%m-%d'),
                            len(self.final_columns),
                            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            os.path.basename(self.project_tracker_path),
                            len(self.final_output_data[self.final_output_data['Item Number'].astype(str).str.strip() != '']),
                            len(self.final_output_data[self.final_output_data['HUGO ID'].astype(str).str.strip() != ''])
                        ]
                    }
                    
                    summary_df = pd.DataFrame(summary_data)
                    summary_df.to_excel(writer, sheet_name='Summary', index=False)
                    
                    # Format sheets
                    workbook = writer.book
                    header_format = workbook.add_format({
                        'bold': True,
                        'bg_color': '#E0E0E0',
                        'font_color': '#000000',
                        'align': 'center'
                    })
                    
                    # Format main sheet
                    worksheet = writer.sheets['Final Data']
                    for col_num, value in enumerate(self.final_columns):
                        worksheet.write(0, col_num, value, header_format)
                        if 'name' in value.lower() or 'description' in value.lower():
                            worksheet.set_column(col_num, col_num, 25)
                        elif 'date' in value.lower():
                            worksheet.set_column(col_num, col_num, 15)
                        else:
                            worksheet.set_column(col_num, col_num, 12)
                
                output_files.append(final_file)
                self.log_message(f"Main output saved: {os.path.basename(final_file)}")
            
            self.log_message(f"Total files saved: {len(output_files)}")
            return output_files
            
        except Exception as e:
            self.log_message(f"Save error: {str(e)}")
            return []
    
    # ========== GUI METHODS ==========
    
    def setup_gui(self):
        """Setup simple, functional GUI"""
        # Title
        title_frame = tk.Frame(self.root, bg='white')
        title_frame.pack(fill='x', pady=10)
        
        title_label = tk.Label(title_frame, text="Automated Data Processor", 
                              font=('Arial', 16, 'bold'), bg='white', fg='black')
        title_label.pack()
        
        subtitle_label = tk.Label(title_frame, text="3-Step Data Processing Workflow", 
                                 font=('Arial', 10), bg='white', fg='gray')
        subtitle_label.pack()
        
        # Main content
        main_frame = tk.Frame(self.root, bg='white')
        main_frame.pack(fill='both', expand=True, padx=20, pady=10)
        
        # Step 1: Project Tracker Selection
        step1_frame = tk.LabelFrame(main_frame, text="Step 1: Select Project Tracker", 
                                   font=('Arial', 10, 'bold'), bg='white')
        step1_frame.pack(fill='x', pady=(0, 10))
        
        tracker_info = tk.Label(step1_frame, text="Choose your Excel project tracker file to begin processing",
                               font=('Arial', 9), bg='white', fg='gray')
        tracker_info.pack(anchor='w', padx=10, pady=(5, 5))
        
        tracker_btn_frame = tk.Frame(step1_frame, bg='white')
        tracker_btn_frame.pack(fill='x', padx=10, pady=(0, 5))
        
        self.browse_btn = tk.Button(tracker_btn_frame, text="Browse for Project Tracker", 
                                   command=self.select_project_tracker,
                                   bg='lightblue', fg='black', font=('Arial', 9))
        self.browse_btn.pack(side='left')
        
        self.tracker_status = tk.Label(step1_frame, text="No file selected", 
                                      font=('Arial', 9), bg='white', fg='red')
        self.tracker_status.pack(anchor='w', padx=10, pady=(0, 10))
        
        # Step 2: Date Range Selection
        step2_frame = tk.LabelFrame(main_frame, text="Step 2: Select Date Range", 
                                   font=('Arial', 10, 'bold'), bg='white')
        step2_frame.pack(fill='x', pady=(0, 10))
        
        date_info = tk.Label(step2_frame, text="Choose the date range for filtering artwork release dates",
                            font=('Arial', 9), bg='white', fg='gray')
        date_info.pack(anchor='w', padx=10, pady=(5, 5))
        
        # Date inputs
        date_input_frame = tk.Frame(step2_frame, bg='white')
        date_input_frame.pack(fill='x', padx=10, pady=(0, 10))
        
        # Start date
        start_frame = tk.Frame(date_input_frame, bg='white')
        start_frame.pack(fill='x', pady=(0, 5))
        
        tk.Label(start_frame, text="Start Date (YYYY-MM-DD):", font=('Arial', 9), bg='white').pack(side='left')
        self.start_date_var = tk.StringVar()
        start_entry = tk.Entry(start_frame, textvariable=self.start_date_var, font=('Arial', 9))
        start_entry.pack(side='left', padx=(10, 0))
        
        # End date
        end_frame = tk.Frame(date_input_frame, bg='white')
        end_frame.pack(fill='x', pady=(0, 10))
        
        tk.Label(end_frame, text="End Date (YYYY-MM-DD):", font=('Arial', 9), bg='white').pack(side='left')
        self.end_date_var = tk.StringVar()
        end_entry = tk.Entry(end_frame, textvariable=self.end_date_var, font=('Arial', 9))
        end_entry.pack(side='left', padx=(10, 0))
        
        # Quick presets
        presets_frame = tk.Frame(step2_frame, bg='white')
        presets_frame.pack(fill='x', padx=10, pady=(0, 10))
        
        tk.Label(presets_frame, text="Quick Presets:", font=('Arial', 9, 'bold'), bg='white').pack(anchor='w')
        
        preset_buttons_frame = tk.Frame(presets_frame, bg='white')
        preset_buttons_frame.pack(fill='x', pady=(5, 0))
        
        def set_last_30():
            current_date = datetime.now().date()
            start_date = current_date - pd.Timedelta(days=30)
            self.start_date_var.set(start_date.strftime('%Y-%m-%d'))
            self.end_date_var.set(current_date.strftime('%Y-%m-%d'))
        
        def set_last_90():
            current_date = datetime.now().date()
            start_date = current_date - pd.Timedelta(days=90)
            self.start_date_var.set(start_date.strftime('%Y-%m-%d'))
            self.end_date_var.set(current_date.strftime('%Y-%m-%d'))
        
        def set_this_year():
            current_date = datetime.now().date()
            start_date = datetime(current_date.year, 1, 1).date()
            self.start_date_var.set(start_date.strftime('%Y-%m-%d'))
            self.end_date_var.set(current_date.strftime('%Y-%m-%d'))
        
        tk.Button(preset_buttons_frame, text="Last 30 Days", command=set_last_30,
                 bg='lightgray', fg='black', font=('Arial', 8)).pack(side='left', padx=(0, 5))
        tk.Button(preset_buttons_frame, text="Last 90 Days", command=set_last_90,
                 bg='lightgray', fg='black', font=('Arial', 8)).pack(side='left', padx=(0, 5))
        tk.Button(preset_buttons_frame, text="This Year", command=set_this_year,
                 bg='lightgray', fg='black', font=('Arial', 8)).pack(side='left')
        
        # Apply button
        self.apply_btn = tk.Button(step2_frame, text="Apply Date Filter & Start Processing", 
                                  command=self.apply_date_filter, state='disabled',
                                  bg='green', fg='white', font=('Arial', 10, 'bold'))
        self.apply_btn.pack(pady=10)
        
        # Step 3: Output Location
        step3_frame = tk.LabelFrame(main_frame, text="Step 3: Output Location", 
                                   font=('Arial', 10, 'bold'), bg='white')
        step3_frame.pack(fill='x', pady=(0, 10))
        
        output_info = tk.Label(step3_frame, text="All processed files will be saved to your Desktop",
                              font=('Arial', 9), bg='white', fg='gray')
        output_info.pack(anchor='w', padx=10, pady=(5, 5))
        
        output_path = tk.Label(step3_frame, text=f"Output Folder: {self.output_folder}",
                              font=('Arial', 8), bg='white', fg='blue')
        output_path.pack(anchor='w', padx=10, pady=(0, 5))
        
        self.open_folder_btn = tk.Button(step3_frame, text="Open Output Folder", 
                                        command=self.open_output_folder, state='disabled',
                                        bg='orange', fg='white', font=('Arial', 9))
        self.open_folder_btn.pack(pady=(0, 10))
        
        # Status and Progress
        status_frame = tk.Frame(main_frame, bg='white')
        status_frame.pack(fill='x', pady=(10, 0))
        
        self.status_label = tk.Label(status_frame, text="Ready to process data", 
                                    font=('Arial', 9, 'bold'), bg='white', fg='blue')
        self.status_label.pack()
        
        self.progress_bar = ttk.Progressbar(status_frame, mode='indeterminate')
        self.progress_bar.pack(fill='x', pady=(5, 0))
        
        # Set default dates
        set_last_90()
    
    def select_project_tracker(self):
        """Select project tracker file"""
        file_path = filedialog.askopenfilename(
            title="Select Project Tracker Excel File",
            filetypes=[("Excel files", "*.xlsx"), ("Excel files", "*.xls"), ("All files", "*.*")],
            initialdir=os.path.dirname(self.default_project_tracker_path) if os.path.exists(self.default_project_tracker_path) else os.path.expanduser("~")
        )
        
        if file_path:
            self.project_tracker_path = file_path
            filename = os.path.basename(file_path)
            
            self.tracker_status.config(text=f"Selected: {filename}", fg='green')
            self.apply_btn.config(state='normal')
            self.log_message(f"Project tracker selected: {filename}")
    
    def apply_date_filter(self):
        """Apply date filter and start processing"""
        try:
            start_str = self.start_date_var.get()
            end_str = self.end_date_var.get()
            
            if not start_str or not end_str:
                messagebox.showerror("Error", "Please enter both start and end dates")
                return
            
            start_date = datetime.strptime(start_str, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_str, '%Y-%m-%d').date()
            
            if start_date > end_date:
                messagebox.showerror("Error", "Start date must be before or equal to end date")
                return
            
            # Disable apply button during processing
            self.apply_btn.config(state='disabled')
            
            # Start automated processing
            self.run_automated_workflow(start_date, end_date)
            
        except ValueError:
            messagebox.showerror("Error", "Please enter dates in YYYY-MM-DD format")
        except Exception as e:
            messagebox.showerror("Error", f"Error starting processing: {str(e)}")
    
    def update_status(self, message):
        """Update status label"""
        self.status_label.config(text=message)
        self.root.update()
    
    def open_output_folder(self):
        """Open output folder"""
        try:
            if platform.system() == 'Windows':
                os.startfile(self.output_folder)
            elif platform.system() == 'Darwin':
                os.system(f'open "{self.output_folder}"')
            else:
                os.system(f'xdg-open "{self.output_folder}"')
        except Exception as e:
            messagebox.showerror("Error", f"Could not open folder: {str(e)}")

def main():
    """Main function"""
    try:
        # Check required packages
        required_packages = ['pandas', 'openpyxl', 'xlsxwriter', 'numpy']
        missing_packages = []
        
        for package in required_packages:
            try:
                __import__(package)
            except ImportError:
                missing_packages.append(package)
        
        if missing_packages:
            # Show error in GUI if packages are missing
            root = tk.Tk()
            root.withdraw()
            messagebox.showerror(
                "Missing Dependencies", 
                f"Missing required packages:\n{', '.join(missing_packages)}\n\n"
                f"Please install them using:\npip install {' '.join(missing_packages)}"
            )
            return
        
        # Create and run application
        app = AutomatedDataProcessor()
        if hasattr(app, 'root'):  # Only run if SharePoint access was granted
            app.root.mainloop()
        
    except Exception as e:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("Application Error", f"Application startup error:\n{str(e)}")

if __name__ == "__main__":
    main()

# ========== EXECUTABLE CREATION INSTRUCTIONS ==========
"""
WINDOWS EXECUTABLE (.exe):
pyinstaller --onefile --windowed --name "AutomatedDataProcessor" \
--hidden-import pandas._libs.tslibs.timedeltas \
--hidden-import pandas._libs.tslibs.np_datetime \
--hidden-import pandas._libs.tslibs.nattype \
--hidden-import pandas._libs.reduction \
--hidden-import openpyxl.cell._writer \
--hidden-import xlsxwriter \
AutomatedDataProcessor.py

MAC EXECUTABLE (.app):
pyinstaller --onefile --windowed --name "AutomatedDataProcessor" \
--hidden-import pandas._libs.tslibs.timedeltas \
--hidden-import pandas._libs.tslibs.np_datetime \
--hidden-import pandas._libs.tslibs.nattype \
--hidden-import pandas._libs.reduction \
--hidden-import openpyxl.cell._writer \
--hidden-import xlsxwriter \
AutomatedDataProcessor.py

Test executables on clean machines without Python to ensure all dependencies are bundled.
"""