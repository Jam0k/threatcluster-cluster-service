#!/usr/bin/env python3
"""
Entity Import Script

Imports manual cybersecurity entities from JSON files into the database.
Processes all .json files in the src/database/entities directory.

Usage:
    Run from the project root directory (cluster-service):
    $ python -m src.database.sql.import_entities
"""
import os
import json
import psycopg2
import logging
from pathlib import Path
from typing import Dict, List, Tuple

from src.config.settings import settings


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EntityImporter:
    """Imports manual entities from JSON files into the database."""
    
    def __init__(self):
        """Initialize the entity importer."""
        self.database_url = settings.database_url
        self.entities_dir = Path(__file__).parent.parent / "entities"
        self.stats = {
            'files_processed': 0,
            'files_empty': 0,
            'entities_inserted': 0,
            'entities_updated': 0,
            'errors': 0
        }
    
    def get_json_files(self) -> List[Path]:
        """Get all JSON files from the entities directory."""
        if not self.entities_dir.exists():
            raise FileNotFoundError(f"Entities directory not found: {self.entities_dir}")
        
        json_files = list(self.entities_dir.glob("*.json"))
        logger.info(f"Found {len(json_files)} JSON files in {self.entities_dir}")
        return json_files
    
    def load_entities_from_file(self, file_path: Path) -> Tuple[str, List[Dict]]:
        """Load entities from a JSON file."""
        category = file_path.stem  # filename without .json extension
        
        try:
            with open(file_path, 'r') as f:
                content = f.read().strip()
                
                if not content:
                    logger.warning(f"Empty file: {file_path.name}")
                    self.stats['files_empty'] += 1
                    return category, []
                
                entities = json.loads(content)
                
                if not isinstance(entities, list):
                    logger.error(f"Invalid format in {file_path.name}: expected list, got {type(entities)}")
                    return category, []
                
                logger.info(f"Loaded {len(entities)} entities from {file_path.name}")
                return category, entities
                
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error in {file_path.name}: {e}")
            self.stats['errors'] += 1
            return category, []
        except Exception as e:
            logger.error(f"Error reading {file_path.name}: {e}")
            self.stats['errors'] += 1
            return category, []
    
    def import_entities(self, category: str, entities: List[Dict], cursor) -> Tuple[int, int]:
        """Import entities into the database."""
        inserted = 0
        updated = 0
        
        for entity in entities:
            try:
                name = entity.get('entities_name')
                weight = entity.get('entities_importance_weight')
                
                if not name:
                    logger.warning(f"Missing entities_name in {category}")
                    continue
                
                if weight is None:
                    logger.warning(f"Missing entities_importance_weight for {name} in {category}")
                    continue
                
                # Insert or update entity
                cursor.execute("""
                    INSERT INTO cluster_data.entities 
                    (entities_name, entities_category, entities_source, entities_importance_weight)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (entities_name, entities_category) 
                    DO UPDATE SET 
                        entities_importance_weight = EXCLUDED.entities_importance_weight,
                        entities_source = EXCLUDED.entities_source
                    RETURNING (xmax = 0) AS inserted
                """, (name, category, 'manual', weight))
                
                result = cursor.fetchone()
                if result[0]:  # inserted (not updated)
                    inserted += 1
                else:
                    updated += 1
                
            except Exception as e:
                logger.error(f"Error inserting entity {entity} in {category}: {e}")
                self.stats['errors'] += 1
        
        return inserted, updated
    
    def run_import(self):
        """Run the complete import process."""
        logger.info("Starting entity import process")
        
        # Get all JSON files
        json_files = self.get_json_files()
        if not json_files:
            logger.warning("No JSON files found to process")
            return
        
        # Connect to database
        conn = psycopg2.connect(self.database_url)
        cursor = conn.cursor()
        
        try:
            # Process each file
            for file_path in sorted(json_files):
                logger.info(f"\nProcessing: {file_path.name}")
                self.stats['files_processed'] += 1
                
                # Load entities from file
                category, entities = self.load_entities_from_file(file_path)
                
                if not entities:
                    continue
                
                # Import entities
                inserted, updated = self.import_entities(category, entities, cursor)
                
                self.stats['entities_inserted'] += inserted
                self.stats['entities_updated'] += updated
                
                logger.info(f"  Category: {category}")
                logger.info(f"  Inserted: {inserted}, Updated: {updated}")
            
            # Commit all changes
            conn.commit()
            logger.info("\nDatabase changes committed successfully")
            
        except Exception as e:
            logger.error(f"Import process error: {e}")
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()
        
        # Print summary
        self.print_summary()
    
    def print_summary(self):
        """Print import summary statistics."""
        print("\n" + "=" * 60)
        print("Entity Import Summary")
        print("=" * 60)
        print(f"Files processed: {self.stats['files_processed']}")
        print(f"Empty files: {self.stats['files_empty']}")
        print(f"Entities inserted: {self.stats['entities_inserted']}")
        print(f"Entities updated: {self.stats['entities_updated']}")
        print(f"Total entities processed: {self.stats['entities_inserted'] + self.stats['entities_updated']}")
        print(f"Errors: {self.stats['errors']}")
        print("=" * 60)
        
        # Show entity counts by category
        self.show_category_counts()
    
    def show_category_counts(self):
        """Show entity counts by category in the database."""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT entities_category, COUNT(*) as count
                FROM cluster_data.entities
                WHERE entities_source = 'manual'
                GROUP BY entities_category
                ORDER BY entities_category
            """)
            
            results = cursor.fetchall()
            
            if results:
                print("\nEntity counts by category:")
                for category, count in results:
                    print(f"  {category}: {count}")
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error fetching category counts: {e}")


def main():
    """Run the entity import process."""
    importer = EntityImporter()
    
    try:
        importer.run_import()
        logger.info("Entity import completed successfully")
        return 0
    except Exception as e:
        logger.error(f"Entity import failed: {e}")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())