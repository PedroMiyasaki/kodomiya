import argparse
import os
import sys
import importlib.util


# Add project root to sys.path for import resolution
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from src.pipelines.resources.config_loader import config


def import_module_from_path(module_name, file_path):
    """Import a module dynamically by its file path."""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    
    return module


def run_scraping_pipeline(source, max_pages=None):
    """Run a specific scraping pipeline or all of them."""
    pipelines_dir = os.path.join(os.path.dirname(__file__), "pipelines")
    available_sources = list(config.get_config_value("sources", default={}).keys())
    
    if source != "all" and source not in available_sources:
        print(f"Error: Source '{source}' not found in configuration.")
        print(f"Available sources: {', '.join(available_sources)}")
        return
    
    if max_pages is not None:
        os.environ['KODOMIYA_MAX_PAGES'] = str(max_pages)
        page_limit_msg = f" (limited to {max_pages} pages)"
    else:
        if 'KODOMIYA_MAX_PAGES' in os.environ:
            del os.environ['KODOMIYA_MAX_PAGES']
        page_limit_msg = ""
    
    if source == "all":
        print(f"Running all scrapers{page_limit_msg}: {', '.join(available_sources)}")
        
        for src in available_sources:
            pipeline_path = os.path.join(pipelines_dir, f"pipeline_{src}.py")
            
            if os.path.exists(pipeline_path):
                print(f"\nRunning {src} pipeline...")
                module_name = f"src.pipelines.pipeline_{src}"
                import_module_from_path(module_name, pipeline_path)
            else:
                print(f"Warning: Pipeline file for source '{src}' not found at {pipeline_path}")
    else:
        pipeline_path = os.path.join(pipelines_dir, f"pipeline_{source}.py")
        
        if os.path.exists(pipeline_path):
            print(f"Running {source} pipeline{page_limit_msg}...")
            module_name = f"src.pipelines.pipeline_{source}"
            import_module_from_path(module_name, pipeline_path)
        else:
            print(f"Error: Pipeline file for source '{source}' not found at {pipeline_path}")


def run_clustering_pipeline():
    """Run the clustering analysis pipeline."""
    script_path = os.path.join(os.path.dirname(__file__), "scripts", "run_imoveis_clustering.py")
    
    if os.path.exists(script_path):
        print("Running clustering analysis...")
        module_name = "src.scripts.run_imoveis_clustering"
        clustering_module = import_module_from_path(module_name, script_path)
        clustering_module.main()
    else:
        print(f"Error: Clustering script not found at {script_path}")


def main():
    """Main entry point for the KodoMiya application."""
    parser = argparse.ArgumentParser(description='KodoMiya - Real Estate Analysis Tool')
    parser.add_argument(
        '--pipeline',
        type=str,
        choices=['scraping', 'clustering'],
        required=True,
        help='Which pipeline to run (scraping or clustering)'
    )
    parser.add_argument(
        '--source',
        type=str,
        default='all',
        help='Which source to scrape (zap_imoveis, viva_real, chaves_na_mao, or all)'
    )
    parser.add_argument(
        '--pages',
        type=int,
        default=None,
        help='Maximum number of pages to scrape (default: no limit)'
    )
    
    args = parser.parse_args()
    
    if args.pipeline == 'scraping':
        run_scraping_pipeline(args.source, args.pages)
    elif args.pipeline == 'clustering':
        run_clustering_pipeline()


if __name__ == "__main__":
    main()