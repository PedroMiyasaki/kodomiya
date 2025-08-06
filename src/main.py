import argparse
import os
import sys
import subprocess


def run_script_in_subprocess(script_filename):
    """Executes a script in src/scripts/ as a subprocess."""
    script_path = os.path.join(os.path.dirname(__file__), "scripts", script_filename)
    if not os.path.exists(script_path):
        print(f"Error: Script not found at {script_path}", file=sys.stderr)
        sys.exit(1)
        
    print(f"\n>>> Running script: {script_filename}...")
    try:
        # Using sys.executable to ensure we use the same Python interpreter
        process = subprocess.run(
            [sys.executable, script_path],
            check=True,
            capture_output=True,
            text=True,
            encoding='utf-8'
        )
        if process.stdout:
            print(process.stdout)
        if process.stderr:
            print(process.stderr, file=sys.stderr)
        print(f">>> Finished script: {script_filename}\n")
    except subprocess.CalledProcessError as e:
        print(f"--- ERROR in {script_filename} ---", file=sys.stderr)
        print(e.stdout)
        print(e.stderr, file=sys.stderr)
        print(f"--- Script {script_filename} failed. Aborting pipeline. ---", file=sys.stderr)
        sys.exit(1)


def main():
    """Main entry point for the KodoMiya application."""
    parser = argparse.ArgumentParser(description='KodoMiya - Real Estate Analysis Tool')
    parser.add_argument(
        'mode',
        type=str,
        choices=['train', 'inference'],
        help='Execution mode: "train" for a full run (scraping, deduplication, training, analysis) or "inference" for a run without model training.'
    )
    parser.add_argument(
        '--pages',
        type=int,
        default=None,
        help='Maximum number of pages to scrape for each source (default: no limit)'
    )
    
    args = parser.parse_args()
    
    # Set max pages environment variable if provided
    if args.pages:
        os.environ['KODOMIYA_MAX_PAGES'] = str(args.pages)
        print(f"Page limit set to {args.pages} for scraping.")

    print(f"--- Starting execution in {args.mode.upper()} mode ---")
    
    # Common steps for both modes
    run_script_in_subprocess("run_scrapping_pipelines.py")
    run_script_in_subprocess("run_deduplication_pipeline.py")

    if args.mode == 'train':
        print("--- Running training and analysis ---")
        run_script_in_subprocess("run_model_training.py")
        run_script_in_subprocess("run_pre_analysis.py")
    else: # inference mode
        print("--- Running analysis ---")
        run_script_in_subprocess("run_pre_analysis.py")
    
    print(f"--- Pipeline execution in {args.mode.upper()} mode finished successfully ---")

    # Clean up environment variable
    if 'KODOMIYA_MAX_PAGES' in os.environ:
        del os.environ['KODOMIYA_MAX_PAGES']


if __name__ == "__main__":
    # Add project root to sys.path for import resolution
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    main()