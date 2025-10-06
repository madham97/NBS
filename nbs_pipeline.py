import os
import json
from pathlib import Path
from typing import Dict, List, Optional, Union
import pandas as pd
from bs4 import BeautifulSoup
import openai
from tqdm import tqdm

class NBSDataPipeline:
    def __init__(self, api_key: str):
        """
        Initialize the NBS Data Pipeline.
        
        Args:
            api_key (str): OpenAI API key for GPT access
        """
        self.api_key = api_key
        openai.api_key = api_key
        
        # Define the schema for our NBS database
        self.schema = {
            'title': str,
            'summary': str,
            'status': str,
            'location_name': str,
            'country': str,
            'scale': str,
            'solution_types': list,
            'challenges_addressed': list,
            'health_linkages_primary': list,
            'impacts': list,
            'governance': str,
            'url_source': str,
            'environmental_context': str
        }
        
        # Valid values for certain fields
        self.valid_status = {'planned', 'in-progress', 'completed', 'ongoing', 'unknown'}
        
    def parse_html(self, file_path: str) -> str:
        """
        Parse HTML content from a file.
        
        Args:
            file_path (str): Path to the HTML file
            
        Returns:
            str: Extracted text content
        """
        with open(file_path, 'r', encoding='utf-8') as file:
            soup = BeautifulSoup(file.read(), 'html.parser')
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
            return soup.get_text(separator=' ', strip=True)
    
    def create_extraction_prompt(self, text_content: str) -> str:
        """
        Create a prompt for the LLM to extract information according to our schema.
        
        Args:
            text_content (str): The text content from which to extract information
            
        Returns:
            str: Formatted prompt for the LLM
        """
        prompt = f"""You are a precise data extraction system for Nature-based Solutions projects. Extract only explicitly stated information from the provided text according to the schema below.

CRITICAL INSTRUCTIONS:
- Never guess or make assumptions - use "unknown" for missing information
- For string fields: use "unknown" if not found
- For array fields: use empty list [] if not found
- Extract information ONLY from the provided text
- Keep arrays concise - use key phrases rather than long sentences
- For status: use ONLY planned|in-progress|completed|ongoing|unknown
- For scale: use ONLY site|neighborhood|city|watershed|regional|unknown
- For environmental_context: use ONLY urban|coastal|wetland|forest|agricultural|unknown

SCHEMA WITH EXAMPLES:
1. title: Project name exactly as given in source
   Example: "Urban Wetland Restoration in Copenhagen"

2. summary: 2-4 concise sentences describing purpose, actions, and context
   Example: "Restoration of 5 hectares of urban wetlands in Copenhagen to reduce flooding risk. The project involves removing invasive species and replanting native vegetation to create natural water retention areas."

3. status: Current project stage
   Example: "completed" (ONLY use: planned|in-progress|completed|ongoing|unknown)

4. location_name: Most specific place name mentioned
   Example: "Amager District, Copenhagen"

5. country: Plain country name
   Example: "Denmark"

6. scale: Geographic scope
   Example: "neighborhood" (ONLY use: site|neighborhood|city|watershed|regional|unknown)

7. solution_types: Array of NBS categories implemented
   Example: ["urban wetlands", "native vegetation restoration", "green corridors"]

8. challenges_addressed: Array of main problems being solved
   Example: ["flooding", "biodiversity loss", "urban heat"]

9. health_linkages_primary: Array of direct health outcomes
   Example: ["reduced heat stress", "improved air quality", "increased physical activity"]

10. impacts: Array of documented outcomes
    Example: ["30% reduction in local flooding", "15% increase in biodiversity", "improved community wellbeing"]

11. governance: Implementation/maintenance responsibility 
    Example: "Copenhagen Municipality in partnership with local community groups"

12. url_source: Original source URL
    Example: "https://oppla.eu/casestudy/21553"

13. environmental_context: Broad context
    Example: "urban" (ONLY use: urban|coastal|wetland|forest|agricultural|unknown)

SOURCE TEXT TO ANALYZE:
{text_content}

RESPONSE FORMAT:
Return a clean JSON object with exactly these 13 fields. Example:
{{
    "title": "Urban Wetland Restoration in Copenhagen",
    "summary": "Restoration of 5 hectares of urban wetlands...",
    "status": "completed",
    ...
}}"""
        return prompt
    
    def extract_info_with_llm(self, prompt: str) -> Dict:
        """
        Use GPT to extract structured information from text.
        
        Args:
            prompt (str): The formatted prompt for GPT
            
        Returns:
            Dict: Extracted information in our schema format
        """
        try:
            response = openai.ChatCompletion.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are a precise data extraction assistant. Extract only explicitly stated information and return unknown for missing data."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.0  # Use deterministic outputs
            )
            return json.loads(response.choices[0].message.content)
        except Exception as e:
            print(f"Error in LLM extraction: {e}")
            return None
    
    def load_existing_data(self, file_path: str) -> pd.DataFrame:
        """
        Load existing data from CSV file if it exists.
        
        Args:
            file_path (str): Path to the CSV file
            
        Returns:
            pd.DataFrame: Existing data or empty DataFrame if file doesn't exist
        """
        try:
            if os.path.exists(file_path):
                return pd.read_csv(file_path, encoding='utf-8-sig')
            return pd.DataFrame()
        except Exception as e:
            print(f"Warning: Could not load existing data from {file_path}: {e}")
            return pd.DataFrame()

    def validate_entry(self, entry: Dict) -> Dict:
        """
        Validate and clean extracted data according to our schema.
        
        Args:
            entry (Dict): The extracted data entry
            
        Returns:
            Dict: Validated and cleaned data entry
        """
        cleaned = {}
        
        # Valid values for controlled fields
        valid_scales = {'site', 'neighborhood', 'city', 'watershed', 'regional', 'unknown'}
        valid_env_contexts = {'urban', 'coastal', 'wetland', 'forest', 'agricultural', 'unknown'}
        
        for field, field_type in self.schema.items():
            value = entry.get(field)
            
            # Handle missing or None values
            if value is None:
                cleaned[field] = [] if field_type == list else 'unknown'
                continue
                
            # Validate and clean based on field
            if field == 'status':
                # Ensure status is one of the valid values
                cleaned[field] = value.lower() if value.lower() in self.valid_status else 'unknown'
                
            elif field == 'scale':
                # Validate geographic scale
                cleaned[field] = value.lower() if value.lower() in valid_scales else 'unknown'
                
            elif field == 'environmental_context':
                # Validate environmental context
                cleaned[field] = value.lower() if value.lower() in valid_env_contexts else 'unknown'
                
            elif field_type == list:
                # Clean and validate array fields
                if isinstance(value, list):
                    # Remove empty strings and duplicates, ensure all items are strings
                    cleaned_list = []
                    seen = set()
                    for item in value:
                        if item and str(item).strip():
                            cleaned_item = str(item).strip().lower()
                            if cleaned_item not in seen:
                                cleaned_list.append(cleaned_item)
                                seen.add(cleaned_item)
                    cleaned[field] = cleaned_list
                else:
                    cleaned[field] = []
                    
            elif field == 'summary':
                # Ensure summary is a proper string and not too long
                cleaned[field] = str(value).strip()
                # If summary is too long, truncate it to roughly 2-4 sentences
                if len(cleaned[field].split('.')) > 4:
                    sentences = cleaned[field].split('.')[:4]
                    cleaned[field] = '. '.join(sentence.strip() for sentence in sentences) + '.'
                    
            else:
                # Clean and validate string fields
                cleaned[field] = str(value).strip() if str(value).strip() else 'unknown'
        
        return cleaned
    
    def process_directory(self, directory_path: str, output_path: str, source_type: str = None) -> pd.DataFrame:
        """
        Process all HTML files in a directory and save results incrementally.
        
        Args:
            directory_path (str): Path to directory containing HTML files
            output_path (str): Path to save the CSV file
            source_type (str): Source of the data ('oppla' or 'unacity') for metadata
            
        Returns:
            pd.DataFrame: Processed data in DataFrame format
        """
        # Load existing data if available
        existing_df = self.load_existing_data(output_path)
        existing_urls = set(existing_df['url_source'].tolist()) if not existing_df.empty else set()
        
        entries = []
        if not existing_df.empty:
            entries = existing_df.to_dict('records')
        
        directory = Path(directory_path)
        html_files = list(directory.glob('*.html'))
        
        # Load metadata if available
        metadata = None
        metadata_file = directory / 'download_metadata.json'
        if metadata_file.exists():
            try:
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
            except Exception as e:
                print(f"Warning: Could not load metadata from {metadata_file}: {e}")
        
        # Process each HTML file
        for file_path in tqdm(html_files, desc=f"Processing {source_type or 'unknown'} files"):
            try:
                # First check if we already have this file's URL in our dataset
                url = 'unknown'
                if metadata and 'successful_files' in metadata:
                    file_name = file_path.name
                    for entry in metadata['successful_files']:
                        if entry.get('filename') == file_name:
                            url = entry.get('link', 'unknown')
                            break
                
                # Skip if URL already exists in our dataset
                if url != 'unknown' and url in existing_urls:
                    print(f"Skipping {file_path} - URL already processed: {url}")
                    continue
                
                # Now we can proceed with content extraction and LLM processing
                text_content = self.parse_html(str(file_path))
                
                if not text_content or len(text_content.strip()) < 100:
                    print(f"Warning: Insufficient content in {file_path}")
                    continue
                
                # Create and send prompt to LLM
                prompt = self.create_extraction_prompt(text_content)
                extracted_info = self.extract_info_with_llm(prompt)
                
                if extracted_info:
                    # Set the URL we found earlier
                    extracted_info['url_source'] = url
                    
                    # Add source information
                    if source_type:
                        extracted_info['data_source'] = source_type
                    
                    # Add file reference
                    extracted_info['source_file'] = str(file_path)
                    
                    # Validate and clean the extracted data
                    validated_entry = self.validate_entry(extracted_info)
                    entries.append(validated_entry)
                    
                    # Save incrementally after each successful processing
                    df = pd.DataFrame(entries)
                    self.save_data(df, output_path, 'csv')
                    print(f"Saved data after processing {file_path}")
                else:
                    print(f"Warning: No data extracted from {file_path}")
                    
            except Exception as e:
                print(f"Error processing {file_path}: {e}")
        
        if not entries:
            print(f"Warning: No entries were successfully processed from {directory_path}")
            return pd.DataFrame()
        
        # Create DataFrame with all fields from our schema
        df = pd.DataFrame(entries)
        
        # Ensure all schema fields exist
        for field in self.schema.keys():
            if field not in df.columns:
                df[field] = 'unknown' if self.schema[field] == str else []
        
        # Add metadata columns
        df['processed_date'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        if source_type:
            df['data_source'] = source_type
        
        # Save processing stats
        stats = {
            'total_files': len(html_files),
            'processed_files': len(entries),
            'success_rate': f"{(len(entries) / len(html_files) * 100):.1f}%",
            'processed_date': df['processed_date'].iloc[0],
            'source_type': source_type or 'unknown'
        }
        
        try:
            stats_file = directory / 'processing_stats.json'
            with open(stats_file, 'w', encoding='utf-8') as f:
                json.dump(stats, f, indent=2)
        except Exception as e:
            print(f"Warning: Could not save processing stats: {e}")
        
        return df
    
    def save_data(self, df: pd.DataFrame, output_path: str, format: str = 'csv') -> None:
        """
        Save the processed data to a file.
        
        Args:
            df (pd.DataFrame): The processed data
            output_path (str): Path where to save the file
            format (str): Format to save the data in ('csv', 'json', or 'excel')
        """
        # Create output directory if it doesn't exist
        output_dir = os.path.dirname(output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        
        # Save in the specified format
        if format == 'csv':
            # Save as CSV with proper encoding and date formatting
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
        elif format == 'json':
            # Save as JSON with proper formatting
            df.to_json(output_path, orient='records', indent=2, force_ascii=False)
        elif format == 'excel':
            # Save as Excel with formatting
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='NBS Projects')
                
                # Get the worksheet
                worksheet = writer.sheets['NBS Projects']
                
                # Auto-adjust column widths
                for idx, col in enumerate(df.columns):
                    max_length = df[col].astype(str).str.len().max()
                    max_length = max(max_length, len(col)) + 2
                    worksheet.column_dimensions[chr(65 + idx)].width = min(max_length, 50)
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        # Save column descriptions
        field_descriptions = {
            'title': 'Project name as given in the source',
            'summary': '2-4 sentence description of project purpose, actions, and context',
            'status': 'Current stage (planned|in-progress|completed|ongoing|unknown)',
            'location_name': 'City/region/named site where the NBS is located',
            'country': 'Country where the NBS is located',
            'scale': 'Geographic scale (site|neighborhood|city|watershed|regional)',
            'solution_types': 'Broad categories of NBS used',
            'challenges_addressed': 'Main problems the project aims to solve',
            'health_linkages_primary': 'Direct health outcomes linked to the NBS',
            'impacts': 'Documented outcomes (environmental, social, or economic)',
            'governance': 'Who is responsible for implementation/maintenance',
            'url_source': 'Link to original project page or source',
            'environmental_context': 'Broad context (urban|coastal|wetland|forest|agricultural)',
            'data_source': 'Source platform (oppla or unacity)',
            'source_file': 'Original HTML file processed',
            'processed_date': 'Date and time of data extraction'
        }
        
        # Save data dictionary
        dict_path = os.path.splitext(output_path)[0] + '_dictionary.json'
        with open(dict_path, 'w', encoding='utf-8') as f:
            json.dump(field_descriptions, f, indent=2)

def main():
    # Initialize the pipeline
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        raise ValueError("Please set the OPENAI_API_KEY environment variable")
    
    pipeline = NBSDataPipeline(api_key)
    
    # Process Oppla data
    oppla_df = pipeline.process_directory('Oppla/raw_html_data', 'oppla_nbs_data.csv', source_type='oppla')
    
    # Process Unacity data
    unacity_df_1 = pipeline.process_directory('Unacity/raw_html_data_1', 'unacity_nbs_data.csv', source_type='unacity')
    unacity_df_2 = pipeline.process_directory('Unacity/raw_html_data_2', 'unacity_nbs_data.csv', source_type='unacity')
    
    # Load final datasets for combining
    oppla_df = pipeline.load_existing_data('oppla_nbs_data.csv')
    unacity_df = pipeline.load_existing_data('unacity_nbs_data.csv')
    
    # Combine all data
    if not oppla_df.empty or not unacity_df.empty:
        all_data = pd.concat([oppla_df, unacity_df], ignore_index=True)
        pipeline.save_data(all_data, 'combined_nbs_data.csv')

if __name__ == "__main__":
    main()