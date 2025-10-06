import os
from nbs_pipeline import NBSDataPipeline
import glob

def main():
    # Initialize the pipeline
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        raise ValueError("Please set the OPENAI_API_KEY environment variable")
    
    pipeline = NBSDataPipeline(api_key)
    
    # Get first 200 files from raw_html_data_1
    files_1 = sorted(glob.glob('Unacity/raw_html_data_1/*.html'))
    files_2 = sorted(glob.glob('Unacity/raw_html_data_2/*.html'))
    
    # Combine and take first 200 files
    all_files = files_1 + files_2
    first_200_files = all_files[:200]
    
    # Create a temporary directory for the first 200 files
    os.makedirs('Unacity/first_200_files', exist_ok=True)
    
    # Copy the first 200 files
    for file_path in first_200_files:
        file_name = os.path.basename(file_path)
        target_path = os.path.join('Unacity/first_200_files', file_name)
        if os.path.exists(target_path):
            os.remove(target_path)
        import shutil
        shutil.copy2(file_path, target_path)
    
    # Process only these 200 files
    unacity_df = pipeline.process_directory('Unacity/first_200_files', 'unacity_first_200_nbs_data.csv', source_type='unacity')

if __name__ == "__main__":
    main()