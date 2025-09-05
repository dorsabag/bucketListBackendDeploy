from fastapi import FastAPI, HTTPException, Query, Path, Body, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from contextlib import asynccontextmanager
import logging
import requests
import time
import os
import asyncio
import urllib.parse
from typing import Dict, Any, List, Optional
from datetime import datetime, date
from pydantic import BaseModel, Field, ValidationError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
class Settings:
    def __init__(self):
        self.notion_api_key = os.getenv("NOTION_API_KEY")
        self.notion_version = os.getenv("NOTION_VERSION", "2022-06-28")
        self.parent_page_id = os.getenv("PARENT_PAGE_ID")
        
        # Existing database IDs
        self.live_shows_db_id = os.getenv("LIVE_SHOWS_DB_ID")
        self.dining_out_db_id = os.getenv("DINING_OUT_DB_ID")
        self.around_world_db_id = os.getenv("AROUND_WORLD_DB_ID")
        self.tv_shows_db_id = os.getenv("TV_SHOWS_DB_ID")
        self.episodes_db_id = os.getenv("EPISODES_DB_ID")
        self.podcasts_db_id = os.getenv("PODCASTS_DB_ID")
        
        # To be created
        self.books_db_id = os.getenv("BOOKS_DB_ID")
        self.movies_db_id = os.getenv("MOVIES_DB_ID")

settings = Settings()

# Notion Service
class NotionService:
    def __init__(self):
        self.base_url = "https://api.notion.com/v1"
        self.headers = {
            "Authorization": f"Bearer {settings.notion_api_key}",
            "Content-Type": "application/json",
            "Notion-Version": settings.notion_version
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def _make_request(self, method: str, url: str, data: Optional[Dict] = None, 
                     retries: int = 3, delay: float = 1.0) -> Dict[str, Any]:
        """Make HTTP request with retry logic and error handling"""
        for attempt in range(retries):
            try:
                if method.upper() == "GET":
                    response = self.session.get(url, params=data)
                elif method.upper() == "POST":
                    response = self.session.post(url, json=data)
                elif method.upper() == "PATCH":
                    response = self.session.patch(url, json=data)
                elif method.upper() == "DELETE":
                    response = self.session.delete(url)
                
                if response.status_code == 429:  # Rate limit exceeded
                    retry_after = int(response.headers.get('Retry-After', delay * (2 ** attempt)))
                    time.sleep(retry_after)
                    continue
                
                response.raise_for_status()
                return {
                    "success": True,
                    "data": response.json(),
                    "status_code": response.status_code
                }
                
            except requests.exceptions.RequestException as e:
                if attempt == retries - 1:  # Last attempt
                    return {
                        "success": False,
                        "error": str(e),
                        "status_code": getattr(e.response, 'status_code', None)
                    }
                time.sleep(delay * (2 ** attempt))
        
        return {"success": False, "error": "Max retries exceeded"}
    
    def query_database(self, database_id: str, filter_conditions: Optional[Dict] = None,
                      sorts: Optional[List[Dict]] = None, page_size: int = 100) -> Dict[str, Any]:
        """Query a Notion database with optional filtering and sorting - gets ALL pages"""
        url = f"{self.base_url}/databases/{database_id}/query"
        
        all_pages = []
        has_more = True
        next_cursor = None
        
        while has_more:
            payload = {"page_size": min(page_size, 100)}  # Notion max is 100
            
            if filter_conditions:
                payload["filter"] = filter_conditions
            
            if sorts:
                payload["sorts"] = sorts
                
            if next_cursor:
                payload["start_cursor"] = next_cursor
            
            result = self._make_request("POST", url, payload)
            
            if not result["success"]:
                return result
            
            # Process and simplify this batch
            pages = result["data"]["results"]
            for page in pages:
                simplified_page = self._simplify_page_data(page)
                all_pages.append(simplified_page)
            
            # Check for more pages
            has_more = result["data"]["has_more"]
            next_cursor = result["data"].get("next_cursor")
            
            # Respect the requested limit
            if len(all_pages) >= page_size:
                all_pages = all_pages[:page_size]
                break
        
        return {
            "success": True,
            "pages": all_pages,
            "has_more": len(all_pages) >= page_size and has_more,
            "next_cursor": next_cursor
        }
    
    def _simplify_page_data(self, page: Dict[str, Any]) -> Dict[str, Any]:
        """Convert Notion's complex page structure to simplified format"""
        simplified = {
            "id": page["id"],
            "created_time": page["created_time"],
            "last_edited_time": page["last_edited_time"],
            "properties": {}
        }
        
        for prop_name, prop_data in page["properties"].items():
            prop_type = prop_data["type"]
            
            if prop_type == "title":
                title_content = prop_data["title"]
                simplified["properties"][prop_name] = title_content[0]["text"]["content"] if title_content else ""
            
            elif prop_type == "rich_text":
                rich_text_content = prop_data["rich_text"]
                simplified["properties"][prop_name] = rich_text_content[0]["text"]["content"] if rich_text_content else ""
            
            elif prop_type == "select":
                select_data = prop_data["select"]
                simplified["properties"][prop_name] = select_data["name"] if select_data else None
            
            elif prop_type == "multi_select":
                multi_select_data = prop_data["multi_select"]
                simplified["properties"][prop_name] = [item["name"] for item in multi_select_data]
            
            elif prop_type == "date":
                date_data = prop_data["date"]
                simplified["properties"][prop_name] = date_data["start"] if date_data else None
            
            elif prop_type == "number":
                simplified["properties"][prop_name] = prop_data["number"]
            
            elif prop_type == "checkbox":
                simplified["properties"][prop_name] = prop_data["checkbox"]
            
            elif prop_type == "files":
                # Handle file/image properties
                files_data = prop_data["files"]
                if files_data:
                    # Get the first file/image URL
                    file_info = files_data[0]
                    if file_info["type"] == "external":
                        simplified["properties"][prop_name] = file_info["external"]["url"]
                    elif file_info["type"] == "file":
                        simplified["properties"][prop_name] = file_info["file"]["url"]
                else:
                    simplified["properties"][prop_name] = None
            
            elif prop_type == "url":
                simplified["properties"][prop_name] = prop_data["url"]
            
            elif prop_type == "relation":
                # Handle relation properties (critical for TV show episodes matching)
                relation_data = prop_data["relation"]
                if relation_data:
                    # Relation fields can contain multiple related items
                    # For now, we'll take the first one and extract its title/name
                    if len(relation_data) > 0:
                        # Each relation item has an 'id' field, but we need the actual title
                        # Since we can't easily resolve the relation here, we'll store the raw data
                        # and let the matching logic handle it
                        simplified["properties"][prop_name] = relation_data
                    else:
                        simplified["properties"][prop_name] = None
                else:
                    simplified["properties"][prop_name] = None
        
        return simplified

# Database Schemas
class DatabaseSchema:
    """Base schema for all bucket list databases"""
    
    @staticmethod
    def get_common_properties() -> Dict[str, Any]:
        return {
            "Title": {"title": {}},
            "Status": {
                "select": {
                    "options": [
                        {"name": "Not Started", "color": "red"},
                        {"name": "In Progress", "color": "yellow"},
                        {"name": "Completed", "color": "green"},
                        {"name": "Cancelled", "color": "gray"}
                    ]
                }
            },
            "Priority": {
                "select": {
                    "options": [
                        {"name": "High", "color": "red"},
                        {"name": "Medium", "color": "yellow"},
                        {"name": "Low", "color": "blue"}
                    ]
                }
            },
            "Added Date": {"date": {}},
            "Completed Date": {"date": {}},
            "Notes": {"rich_text": {}},
            "Rating": {
                "select": {
                    "options": [
                        {"name": "⭐", "color": "red"},
                        {"name": "⭐⭐", "color": "orange"},
                        {"name": "⭐⭐⭐", "color": "yellow"},
                        {"name": "⭐⭐⭐⭐", "color": "green"},
                        {"name": "⭐⭐⭐⭐⭐", "color": "blue"}
                    ]
                }
            }
        }

class BooksSchema(DatabaseSchema):
    @staticmethod
    def get_schema() -> Dict[str, Any]:
        properties = DatabaseSchema.get_common_properties()
        properties.update({
            "Author": {"rich_text": {}},
            "Genre": {
                "select": {
                    "options": [
                        {"name": "Fiction", "color": "blue"},
                        {"name": "Non-Fiction", "color": "green"},
                        {"name": "Biography", "color": "purple"},
                        {"name": "Science", "color": "red"},
                        {"name": "History", "color": "orange"},
                        {"name": "Self-Help", "color": "yellow"}
                    ]
                }
            },
            "Pages": {"number": {}},
            "Started Reading": {"date": {}},
            "Finished Reading": {"date": {}},
            "Recommendation Source": {"rich_text": {}},
            "Key Takeaways": {"rich_text": {}}
        })
        return properties

class MoviesSchema(DatabaseSchema):
    @staticmethod
    def get_schema() -> Dict[str, Any]:
        properties = DatabaseSchema.get_common_properties()
        properties.update({
            "Director": {"rich_text": {}},
            "Release Year": {"number": {}},
            "Genre": {
                "select": {
                    "options": [
                        {"name": "Drama", "color": "blue"},
                        {"name": "Comedy", "color": "yellow"},
                        {"name": "Action", "color": "red"},
                        {"name": "Horror", "color": "gray"},
                        {"name": "Documentary", "color": "green"},
                        {"name": "Sci-Fi", "color": "purple"}
                    ]
                }
            },
            "Runtime": {"number": {}},
            "Streaming Platform": {
                "select": {
                    "options": [
                        {"name": "Netflix", "color": "red"},
                        {"name": "Amazon Prime", "color": "blue"},
                        {"name": "Disney+", "color": "purple"},
                        {"name": "HBO Max", "color": "gray"},
                        {"name": "Hulu", "color": "green"},
                        {"name": "Theater", "color": "yellow"}
                    ]
                }
            },
            "Watched With": {"multi_select": {"options": []}}
        })
        return properties

# Database Creator
class DatabaseCreator:
    
    def initialize_databases(self) -> Dict[str, Any]:
        """Initialize all required databases, creating missing ones"""
        results = {"success": True, "created": [], "existing": [], "errors": []}
        
        if not settings.parent_page_id:
            return {
                "success": False,
                "error": "Parent page ID not configured"
            }
        
        # Check and create each required database
        for category, config in self.required_databases.items():
            try:
                current_id = getattr(settings, f"{category}_db_id", None)
                
                if current_id:
                    results["existing"].append({
                        "category": category,
                        "database_id": current_id
                    })
                else:
                    # Create new database
                    creation_result = self._create_new_database(category, config)
                    if creation_result["success"]:
                        results["created"].append({
                            "category": category,
                            "database_id": creation_result["database_id"],
                            "title": config["title"]
                        })
                        # Update the settings
                        setattr(settings, f"{category}_db_id", creation_result["database_id"])
                    else:
                        results["errors"].append({
                            "category": category,
                            "error": creation_result["error"]
                        })
                        results["success"] = False
                
            except Exception as e:
                logger.error(f"Failed to initialize {category} database: {e}")
                results["errors"].append({
                    "category": category,
                    "error": str(e)
                })
                results["success"] = False
        
        return results
    
    def _create_new_database(self, category: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new database with specified configuration"""
        schema_class = config["schema_class"]
        properties = schema_class.get_schema()
        
        database_payload = {
            "parent": {
                "type": "page_id",
                "page_id": settings.parent_page_id
            },
            "title": [
                {
                    "type": "text",
                    "text": {
                        "content": config["title"],
                        "link": None
                    }
                }
            ],
            "properties": properties,
            "icon": {
                "type": "emoji",
                "emoji": self._get_category_emoji(category)
            }
        }
        
        url = f"{self.notion_service.base_url}/databases"
        result = self.notion_service._make_request("POST", url, database_payload)
        
        if result["success"]:
            database_id = result["data"]["id"]
            logger.info(f"Successfully created {category} database: {database_id}")
            return {
                "success": True,
                "database_id": database_id,
                "database": result["data"]
            }
        else:
            return {
                "success": False,
                "error": result.get("error", "Unknown error")
            }
    
    def _get_category_emoji(self, category: str) -> str:
        """Return appropriate emoji for each category"""
        emoji_map = {
            "books": "📚",
            "movies": "🎬"
        }
        return emoji_map.get(category, "📝")

# CRUD Operations
class BucketListItem(BaseModel):
    title: str
    notes: Optional[str] = None
    
    class Config:
        extra = "allow"  # Allow additional fields for category-specific properties

class BucketListCRUD:
    def __init__(self):
        self.notion_service = NotionService()
        self._update_database_mapping()
    
    def _update_database_mapping(self):
        """Update database mapping with current values"""
        self.database_mapping = {
            "live_shows": settings.live_shows_db_id,
            "dining_out": settings.dining_out_db_id,
            "around_world": settings.around_world_db_id,
            "tv_shows": settings.tv_shows_db_id,
            "episodes": settings.episodes_db_id,  # Keep episodes for TV show relations
            "podcasts": settings.podcasts_db_id,
            "books": settings.books_db_id,
            "movies": settings.movies_db_id
        }
    
    def create_item(self, category: str, item_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new bucket list item in the specified category"""
        try:
            # Validate input data
            bucket_item = BucketListItem(**item_data)
            
            # Get database ID for category, create if missing
            database_id = self._get_or_create_database(category)
            if not database_id:
                if category in ["books", "movies"]:
                    return {
                        "success": False, 
                        "error": f"The {category} database is not set up yet. Please contact your administrator to configure the {category.title()} database in Notion."
                    }
                else:
                    return {"success": False, "error": f"Unknown category: {category}"}
            
            # Transform data to Notion format
            notion_properties = self._transform_to_notion_format(category, bucket_item.dict())
            
            # DEBUG: Log what we're sending to Notion
            logger.info(f"Creating {category} item with properties: {notion_properties}")
            
            # Add current timestamp only for new categories (books, movies) that have this property
            if category in ["books", "movies"] and not notion_properties.get("Added Date"):
                notion_properties["Added Date"] = {
                    "date": {"start": datetime.now().isoformat()}
                }
            
            # Create page payload
            page_payload = {
                "parent": {"database_id": database_id},
                "properties": notion_properties
            }
            
            # DEBUG: Log the full payload
            logger.info(f"Full Notion API payload: {page_payload}")
            
            # Create the page
            url = f"{self.notion_service.base_url}/pages"
            result = self.notion_service._make_request("POST", url, page_payload)
            
            if result["success"]:
                # Simplify response data
                page_data = self.notion_service._simplify_page_data(result["data"])
                logger.info(f"Created {category} item: {bucket_item.title}")
                return {
                    "success": True,
                    "item": page_data,
                    "message": f"Successfully created {category} item"
                }
            
            return result
            
        except ValidationError as e:
            return {"success": False, "error": f"Validation error: {e}"}
        except Exception as e:
            logger.error(f"Error creating {category} item: {e}")
            return {"success": False, "error": str(e)}
    
    def _get_or_create_database(self, category: str) -> Optional[str]:
        """Get database ID for category, creating if necessary"""
        # Refresh database mapping first
        self._update_database_mapping()
        
        database_id = self.database_mapping.get(category)
        
        if database_id:
            return database_id
            
        # For now, return None for books/movies to handle gracefully
        # until user provides proper parent page ID
        if category in ["books", "movies"]:
            logger.warning(f"Database for {category} not configured. Need proper parent page ID to create.")
            return None
                
        return None
    
    def read_items(self, category: str, filters: Optional[Dict] = None, 
                   limit: int = 100) -> Dict[str, Any]:
        """Read bucket list items from specified category with optional filtering"""
        try:
            # Get database ID for category
            database_id = self._get_or_create_database(category)
            if not database_id:
                # For books/movies without database, return empty result
                if category in ["books", "movies"]:
                    return {
                        "success": True,
                        "category": category,
                        "items": [],
                        "count": 0,
                        "has_more": False
                    }
                else:
                    return {"success": False, "error": f"Unknown category: {category}"}
            
            # Query the database without filters
            filter_conditions = None
            
            # Query the database
            result = self.notion_service.query_database(
                database_id=database_id,
                filter_conditions=filter_conditions,
                page_size=limit
            )
            
            if result["success"]:
                logger.info(f"Retrieved {len(result['pages'])} {category} items")
                return {
                    "success": True,
                    "category": category,
                    "items": result["pages"],
                    "count": len(result["pages"]),
                    "has_more": result["has_more"]
                }
            
            return result
            
        except Exception as e:
            logger.error(f"Error reading {category} items: {e}")
            return {"success": False, "error": str(e)}
    
    def update_item(self, category: str, item_id: str, 
                   update_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing bucket list item"""
        try:
            # Transform data to Notion format
            notion_properties = self._transform_to_notion_format(category, update_data)
            
            # DEBUG: Log what we're sending to Notion
            logger.info(f"Updating item {item_id} with data: {update_data}")
            logger.info(f"Transformed to Notion format: {notion_properties}")
            
            # Remove None values to avoid overwriting existing data
            notion_properties = {k: v for k, v in notion_properties.items() if v is not None}
            
            logger.info(f"After filtering None values: {notion_properties}")
            
            # Update payload
            update_payload = {"properties": notion_properties}
            
            # Update the page
            url = f"{self.notion_service.base_url}/pages/{item_id}"
            result = self.notion_service._make_request("PATCH", url, update_payload)
            
            logger.info(f"Notion API response: {result}")
            
            if result["success"]:
                page_data = self.notion_service._simplify_page_data(result["data"])
                logger.info(f"Updated {category} item: {item_id}")
                return {
                    "success": True,
                    "item": page_data,
                    "message": f"Successfully updated {category} item"
                }
            
            return result
            
        except Exception as e:
            logger.error(f"Error updating {category} item {item_id}: {e}")
            return {"success": False, "error": str(e)}
    
    def delete_item(self, category: str, item_id: str) -> Dict[str, Any]:
        """Delete (archive) a bucket list item"""
        try:
            # Archive the page (Notion's version of delete)
            archive_payload = {"archived": True}
            
            url = f"{self.notion_service.base_url}/pages/{item_id}"
            result = self.notion_service._make_request("PATCH", url, archive_payload)
            
            if result["success"]:
                logger.info(f"Deleted {category} item: {item_id}")
                return {
                    "success": True,
                    "message": f"Successfully deleted {category} item"
                }
            
            return result
            
        except Exception as e:
            logger.error(f"Error deleting {category} item {item_id}: {e}")
            return {"success": False, "error": str(e)}
    
    def _transform_to_notion_format(self, category: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform simple data format to Notion's property format"""
        notion_properties = {}
        
        # For existing categories with Hebrew properties, use basic mapping
        if category == "live_shows":
            # Use Hebrew property names that exist in the database
            notion_properties["Name"] = {
                "title": [{"text": {"content": str(data.get("title", ""))}}]
            }
            # Note: live_shows database doesn't have a notes field, so we skip it
            # Handle image_url for live_shows
            if data.get("image_url"):
                notion_properties["Image"] = {
                    "url": str(data["image_url"])
                }
            # Handle category-specific fields for live_shows using Hebrew names
            if data.get("location"):
                notion_properties["מקום"] = {
                    "rich_text": [{"text": {"content": str(data["location"])}}]
                }
            if data.get("date"):
                notion_properties["תאריך"] = {
                    "date": {"start": str(data["date"])}
                }
            if data.get("with_whom"):
                if isinstance(data["with_whom"], list):
                    # Handle array of people for multi-select
                    notion_properties["עם מי הלכתי"] = {
                        "multi_select": [{"name": str(person)} for person in data["with_whom"] if person]
                    }
                else:
                    # Handle single person
                    notion_properties["עם מי הלכתי"] = {
                        "multi_select": [{"name": str(data["with_whom"])}]
                    }
            return notion_properties
        
        # For other existing categories, use enhanced mapping with category-specific fields
        if category in ["dining_out", "around_world", "tv_shows", "episodes", "podcasts"]:
            notion_properties["Name"] = {
                "title": [{"text": {"content": str(data.get("title", ""))}}]
            }
            # Only add Notes field for categories that support it (episodes and some others)
            if data.get("notes") and category in ["episodes"]:
                notion_properties["Notes"] = {
                    "rich_text": [{"text": {"content": str(data["notes"])}}]
                }
            # Handle image_url for existing categories
            if data.get("image_url"):
                notion_properties["Image"] = {
                    "url": str(data["image_url"])
                }
            
            # Handle category-specific fields using actual property names from databases
            if category == "dining_out":
                if data.get("rating"):
                    notion_properties["ציון"] = {  # Hebrew for "rating"
                        "select": {"name": str(data["rating"])}
                    }
                if data.get("cuisine"):
                    notion_properties["קטגוריה"] = {  # Hebrew for "category" - used for cuisine
                        "multi_select": [{"name": str(data["cuisine"])}]
                    }
                # Note: price_range field doesn't exist in dining_out database, so we skip it
            
            elif category == "around_world":
                if data.get("dates"):
                    notion_properties["תאריך"] = {  # Hebrew for "date"
                        "date": {"start": str(data["dates"]).split(" to ")[0] if " to " in str(data["dates"]) else str(data["dates"])}
                    }
                # Note: country field doesn't exist in around_world database, so we skip it
            
            elif category == "tv_shows":
                if data.get("rating"):
                    # Rating is a select field with predefined star options
                    notion_properties["Rating"] = {
                        "select": {"name": str(data["rating"])}
                    }
                if data.get("network"):
                    # Network is a select field, not rich_text
                    notion_properties["Network"] = {
                        "select": {"name": str(data["network"])}
                    }
                if data.get("airing_years"):
                    notion_properties["Airing Years"] = {
                        "rich_text": [{"text": {"content": str(data["airing_years"])}}]
                    }
                if data.get("imdb_link"):
                    notion_properties["IMDb Link"] = {
                        "url": str(data["imdb_link"])
                    }
            
            elif category == "podcasts":
                if data.get("speakers"):
                    notion_properties["דובר/ים"] = {  # Hebrew for "speakers"
                        "rich_text": [{"text": {"content": str(data["speakers"])}}]
                    }
                if data.get("network"):
                    # Network might be a select field in podcasts too
                    notion_properties["Network"] = {
                        "select": {"name": str(data["network"])}
                    }
            
            return notion_properties
        
        # For new categories (books, movies), use full transformation
        for key, value in data.items():
            if value is None:
                continue
                
            # Title property (usually the first field)
            if key.lower() in ['title', 'name']:
                notion_properties["Title"] = {
                    "title": [{"text": {"content": str(value)}}]
                }
            
            # Multi-select properties  
            elif key.lower() in ['genre', 'cuisine_type', 'price_range', 'with_whom', 'עם מי הלכתי']:
                property_name = key.replace('_', ' ').title()
                if isinstance(value, list):
                    # Handle array of values for multi-select
                    notion_properties[property_name] = {
                        "multi_select": [{"name": str(item)} for item in value if item]
                    }
                else:
                    # Handle single value
                    notion_properties[property_name] = {
                        "multi_select": [{"name": str(value)}]
                    }
            
            # Rich text properties
            elif key.lower() in ['notes', 'artist', 'venue', 'restaurant', 'author', 'director']:
                property_name = key.replace('_', ' ').title()
                notion_properties[property_name] = {
                    "rich_text": [{"text": {"content": str(value)}}]
                }
            
            # URL properties (for images)
            elif key.lower() in ['image_url', 'image', 'cover']:
                property_name = "Image" if key.lower() == 'image_url' else key.replace('_', ' ').title()
                notion_properties[property_name] = {
                    "url": str(value) if value else None
                }
            
            # Date properties
            elif key.lower().endswith('_date') or key.lower().endswith('date'):
                property_name = key.replace('_', ' ').title()
                if isinstance(value, (datetime, date)):
                    date_value = value.isoformat()
                else:
                    date_value = str(value)
                notion_properties[property_name] = {
                    "date": {"start": date_value}
                }
            
            # Number properties
            elif key.lower() in ['pages', 'runtime', 'release_year', 'ticket_price']:
                property_name = key.replace('_', ' ').title()
                notion_properties[property_name] = {
                    "number": float(value) if value else 0
                }
        
        return notion_properties

# Global state for database initialization status
app_state = {
    "databases_initialized": False, 
    "initialization_error": None,
    "connected_clients": set()  # Track WebSocket connections
}

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        app_state["connected_clients"].add(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        app_state["connected_clients"].discard(websocket)

    async def broadcast(self, message: dict):
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except:
                disconnected.append(connection)
        
        # Clean up disconnected clients
        for connection in disconnected:
            self.disconnect(connection)

manager = ConnectionManager()

# Notion Webhook Handler
class NotionWebhookHandler:
    def __init__(self):
        self.notion_service = NotionService()
    
    async def setup_webhooks(self):
        """Set up webhooks for all databases"""
        try:
            webhook_url = "https://your-app-domain.com/api/webhooks/notion"  # This would be your actual domain
            
            databases = {
                "live_shows": settings.live_shows_db_id,
                "dining_out": settings.dining_out_db_id,
                "around_world": settings.around_world_db_id,
                "tv_shows": settings.tv_shows_db_id,
                "episodes": settings.episodes_db_id,
                "podcasts": settings.podcasts_db_id,
            }
            
            for category, db_id in databases.items():
                if db_id:
                    await self._create_webhook(db_id, webhook_url, category)
                    
        except Exception as e:
            logger.error(f"Error setting up webhooks: {e}")
    
    async def _create_webhook(self, database_id: str, webhook_url: str, category: str):
        """Create a webhook for a specific database"""
        try:
            payload = {
                "parent": {
                    "type": "database_id",
                    "database_id": database_id
                },
                "url": webhook_url,
                "event_types": ["page.property_updated", "page.created", "page.deleted"]
            }
            
            result = self.notion_service._make_request(
                "POST", 
                f"{self.notion_service.base_url}/webhooks",
                payload
            )
            
            if result["success"]:
                logger.info(f"Webhook created for {category} database")
                return result["data"]
            else:
                logger.error(f"Failed to create webhook for {category}: {result}")
                
        except Exception as e:
            logger.error(f"Error creating webhook for {category}: {e}")

webhook_handler = NotionWebhookHandler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager - handles startup and shutdown events"""
    # Startup
    logger.info("Starting Bucket List Application")
    
    try:
        # Initialize databases
        db_creator = DatabaseCreator()
        init_result = db_creator.initialize_databases()
        
        if init_result["success"]:
            app_state["databases_initialized"] = True
            logger.info(f"Database initialization completed successfully")
            logger.info(f"Created databases: {init_result['created']}")
            logger.info(f"Existing databases: {init_result['existing']}")
            
            # No webhooks needed for local-only setup
            logger.info("Running in local mode - no webhooks needed")
        else:
            app_state["initialization_error"] = init_result["errors"]
            logger.error(f"Database initialization failed: {init_result['errors']}")
    
    except Exception as e:
        app_state["initialization_error"] = str(e)
        logger.error(f"Startup failed: {e}")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Bucket List Application")

# Create FastAPI app
app = FastAPI(
    title="Bucket List Notion Integration",
    description="A comprehensive bucket list application integrated with Notion databases",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models for request/response validation
class BucketListItemCreate(BaseModel):
    title: str = Field(..., min_length=1, max_length=300)
    notes: Optional[str] = Field(None, max_length=2000)
    
    class Config:
        extra = "allow"

class BucketListItemUpdate(BaseModel):
    title: Optional[str] = Field(None, min_length=1, max_length=300)
    notes: Optional[str] = Field(None, max_length=2000)
    
    class Config:
        extra = "allow"

@app.post("/api/admin/add-image-properties")
async def add_image_properties():
    """Add Image URL properties to existing databases"""
    try:
        notion_service = NotionService()
        results = []
        
        databases = {
            "live_shows": settings.live_shows_db_id,
            "dining_out": settings.dining_out_db_id,
            "around_world": settings.around_world_db_id,
            "tv_shows": settings.tv_shows_db_id,
            "episodes": settings.episodes_db_id,
            "podcasts": settings.podcasts_db_id,
        }
        
        for category, db_id in databases.items():
            if db_id:
                try:
                    # Add Image property to the database
                    update_payload = {
                        "properties": {
                            "Image": {
                                "url": {}
                            }
                        }
                    }
                    
                    result = notion_service._make_request(
                        "PATCH",
                        f"{notion_service.base_url}/databases/{db_id}",
                        update_payload
                    )
                    
                    if result["success"]:
                        results.append({
                            "category": category,
                            "status": "success",
                            "message": f"Added Image property to {category}"
                        })
                        logger.info(f"Added Image property to {category} database")
                    else:
                        results.append({
                            "category": category,
                            "status": "error",
                            "message": result.get("error", "Unknown error")
                        })
                        
                except Exception as e:
                    results.append({
                        "category": category,
                        "status": "error", 
                        "message": str(e)
                    })
        
        return {
            "success": True,
            "message": "Image property addition completed",
            "results": results
        }
        
    except Exception as e:
        logger.error(f"Error adding image properties: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time updates"""
    await manager.connect(websocket)
    try:
        while True:
            # Keep connection alive and handle incoming messages
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.post("/api/webhooks/notion")
async def notion_webhook(request: dict = Body(...)):
    """Handle webhooks from Notion"""
    try:
        logger.info(f"Received Notion webhook: {request}")
        
        # Extract information from the webhook
        event_type = request.get("type", "")
        object_type = request.get("object", "")
        
        if event_type in ["page.property_updated", "page.created", "page.deleted"]:
            page_data = request.get("data", {})
            
            # Determine which category this belongs to
            parent = page_data.get("parent", {})
            database_id = parent.get("database_id", "")
            
            # Map database ID to category
            category_mapping = {
                settings.live_shows_db_id: "live_shows",
                settings.dining_out_db_id: "dining_out", 
                settings.around_world_db_id: "around_world",
                settings.tv_shows_db_id: "tv_shows",
                settings.episodes_db_id: "episodes",
                settings.podcasts_db_id: "podcasts"
            }
            
            category = category_mapping.get(database_id, "unknown")
            
            if category != "unknown":
                # Broadcast update to all connected clients
                await manager.broadcast({
                    "type": "notion_update",
                    "event_type": event_type,
                    "category": category,
                    "page_id": page_data.get("id", ""),
                    "timestamp": datetime.utcnow().isoformat(),
                    "message": f"{event_type} in {category} category"
                })
                
                logger.info(f"Broadcasted {event_type} for {category} to {len(manager.active_connections)} clients")
            
        return {"status": "success", "message": "Webhook processed"}
        
    except Exception as e:
        logger.error(f"Error processing Notion webhook: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/api/", response_model=Dict[str, Any])
async def root():
    """Root endpoint providing application status and basic information"""
    return {
        "message": "Bucket List Notion Integration API",
        "version": "1.0.0",
        "status": "running",
        "databases_initialized": app_state["databases_initialized"],
        "documentation": "/docs"
    }

@app.get("/api/health", response_model=Dict[str, Any])
async def health_check():
    """Health check endpoint for monitoring and load balancers"""
    if not app_state["databases_initialized"]:
        raise HTTPException(
            status_code=503,
            detail={
                "status": "unhealthy",
                "reason": "Databases not initialized",
                "error": app_state["initialization_error"]
            }
        )
    
    # Perform basic connectivity test
    try:
        crud_ops = BucketListCRUD()
        # Test connection to one database
        test_result = crud_ops.read_items("live_shows", limit=1)
        
        if test_result["success"]:
            return {
                "status": "healthy",
                "databases_initialized": True,
                "notion_connectivity": "ok",
                "timestamp": datetime.utcnow().isoformat()
            }
        else:
            raise HTTPException(
                status_code=503,
                detail={
                    "status": "unhealthy",
                    "reason": "Notion connectivity failed",
                    "error": test_result["error"]
                }
            )
    
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail={
                "status": "unhealthy",
                "reason": "Health check failed",
                "error": str(e)
            }
        )

@app.get("/api/proxy-image")
async def proxy_notion_image(url: str):
    """Proxy Notion-authenticated images to the frontend"""
    from fastapi.responses import Response
    import urllib.parse
    
    try:
        # Validate that this is a Notion image URL to prevent abuse
        if not url.startswith('https://www.notion.so/image/'):
            raise HTTPException(status_code=400, detail="Invalid image URL")
        
        # Extract the original image URL from the Notion wrapper URL
        # Format: https://www.notion.so/image/{encoded_original_url}?params
        if '/image/' in url:
            # Get the part after /image/
            image_part = url.split('/image/')[1]
            # Split on ? to separate the encoded URL from parameters
            encoded_url = image_part.split('?')[0]
            # URL decode to get the original image URL - decode twice for double-encoded URLs
            original_url = urllib.parse.unquote(urllib.parse.unquote(encoded_url))
            
            logger.info(f"Extracted original URL: {original_url}")
            
            # Skip invalid protocols like attachment:
            if not original_url.startswith(('http://', 'https://')):
                logger.warning(f"Skipping invalid protocol URL: {original_url}")
                # Try the Notion URL directly with API credentials
                headers = {
                    'Authorization': f'Bearer {settings.notion_api_key}',
                    'Notion-Version': settings.notion_version
                }
                
                response = requests.get(url, headers=headers, timeout=10)
                
                if response.status_code == 200:
                    content_type = response.headers.get('content-type', 'image/jpeg')
                    return Response(
                        content=response.content,
                        media_type=content_type,
                        headers={
                            'Cache-Control': 'public, max-age=3600',
                            'Access-Control-Allow-Origin': '*'
                        }
                    )
                else:
                    raise HTTPException(status_code=404, detail="Image not found")
            
            # Try to fetch the original image directly first with better headers
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(original_url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                # Return the image with appropriate headers
                content_type = response.headers.get('content-type', 'image/jpeg')
                return Response(
                    content=response.content,
                    media_type=content_type,
                    headers={
                        'Cache-Control': 'public, max-age=3600',  # Cache for 1 hour
                        'Access-Control-Allow-Origin': '*'
                    }
                )
            else:
                logger.warning(f"Original URL failed with status {response.status_code}, trying Notion URL with API key")
                # If original fails, try the Notion URL with API credentials as fallback
                headers = {
                    'Authorization': f'Bearer {settings.notion_api_key}',
                    'Notion-Version': settings.notion_version
                }
                
                notion_response = requests.get(url, headers=headers, timeout=10)
                
                if notion_response.status_code == 200:
                    content_type = notion_response.headers.get('content-type', 'image/jpeg')
                    return Response(
                        content=notion_response.content,
                        media_type=content_type,
                        headers={
                            'Cache-Control': 'public, max-age=3600',
                            'Access-Control-Allow-Origin': '*'
                        }
                    )
        
        logger.error(f"Failed to fetch image from both original and Notion URLs: {url}")
        raise HTTPException(status_code=404, detail="Image not found")
    
    except requests.RequestException as e:
        logger.error(f"Error fetching image: {url}, error: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to fetch image")
    except Exception as e:
        logger.error(f"Unexpected error proxying image: {url}, error: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/categories/{category}/items", response_model=Dict[str, Any])
async def create_bucket_list_item(
    category: str = Path(..., pattern="^(live_shows|dining_out|around_world|tv_shows|podcasts|books|movies|episodes)$"),
    item: BucketListItemCreate = Body(...)
):
    """Create a new bucket list item in the specified category"""
    try:
        crud_ops = BucketListCRUD()
        result = crud_ops.create_item(category, item.dict())
        
        if result["success"]:
            return {
                "success": True,
                "message": f"Successfully created {category} item",
                "item": result["item"]
            }
        else:
            raise HTTPException(status_code=400, detail=result["error"])
            
    except Exception as e:
        logger.error(f"Error creating {category} item: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/categories/{category}/items", response_model=Dict[str, Any])
async def get_bucket_list_items(
    category: str = Path(..., pattern="^(live_shows|dining_out|around_world|tv_shows|podcasts|books|movies|episodes)$"),
    limit: int = Query(500, ge=1, le=500)
):
    """Get bucket list items from specified category with optional filtering"""
    try:
        crud_ops = BucketListCRUD()
        
        result = crud_ops.read_items(category, filters=None, limit=limit)
        
        if result["success"]:
            items = result["items"]
            
            # For around_world category, filter out child items (cities)
            # Since there are no explicit parent-child relationship fields in the database,
            # we need to use heuristics based on the actual data patterns
            if category == "around_world":
                parent_items = []
                
                for item in items:
                    item_props = item["properties"]
                    item_title = item_props.get("Title") or item_props.get("Name") or ""
                    
                    # Strategy 1: Items with country flag emojis are definitely parents (countries)
                    country_flags = ["🇩🇪", "🇺🇸", "🇫🇷", "🇮🇱", "🇬🇧", "🇮🇹", "🇪🇸", "🇳🇱", "🇨🇭", "🇦🇹", "🇹🇭", "🇩🇰", "🇯🇵", "🇰🇷", "🇨🇳", "🇮🇳", "🇦🇺", "🇨🇦", "🇧🇷", "🇲🇽", "🇷🇺", "🇿🇦"]
                    has_country_flag = any(flag in item_title for flag in country_flags)
                    
                    # Strategy 2: Items that are just country names (longer titles are likely cities)
                    # Countries typically have shorter, simpler names vs "City, Country" format
                    is_short_name = len(item_title.replace(" ", "").replace("🇩🇪", "").replace("🇹🇭", "").replace("🇩🇰", "")) < 15
                    
                    # Strategy 3: Look for city patterns (comma separated, specific city indicators)
                    looks_like_city = (
                        "," in item_title or  # "Berlin, Germany" format
                        "ברלין" in item_title.lower() or  # Specific city names
                        "amsterdam" in item_title.lower() or
                        "copenhagen" in item_title.lower() or
                        "bangkok" in item_title.lower() or
                        "phuket" in item_title.lower() or
                        "krabi" in item_title.lower() or
                        "chiang mai" in item_title.lower() or
                        "pattaya" in item_title.lower() or
                        "samui" in item_title.lower() or  # Add Koh Samui detection
                        "koh" in item_title.lower() or   # Thai island prefix
                        "berlin" in item_title.lower() or
                        "munich" in item_title.lower() or
                        "hamburg" in item_title.lower()
                    )
                    
                    # Classification: Parent if has country flag OR (short name AND not obviously a city)
                    is_parent = has_country_flag or (is_short_name and not looks_like_city)
                    
                    if is_parent:
                        parent_items.append(item)
                        logger.info(f"Around World Parent: {item_title} (flag: {has_country_flag}, short: {is_short_name}, city_pattern: {looks_like_city})")
                    else:
                        logger.info(f"Around World Child: {item_title} (filtered out)")
                
                items = parent_items
                logger.info(f"Around World filtering: {len(parent_items)} parents from {len(result['items'])} total items")
            
            return {
                "success": True,
                "category": category,
                "items": items,
                "count": len(items),
                "has_more": result.get("has_more", False)
            }
        else:
            raise HTTPException(status_code=400, detail=result["error"])
            
    except Exception as e:
        logger.error(f"Error retrieving {category} items: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/categories/{category}/items/{item_id}", response_model=Dict[str, Any])
async def update_bucket_list_item(
    category: str = Path(..., pattern="^(live_shows|dining_out|around_world|tv_shows|podcasts|books|movies|episodes)$"),
    item_id: str = Path(..., min_length=32, max_length=40),
    item: BucketListItemUpdate = Body(...)
):
    """Update an existing bucket list item"""
    try:
        crud_ops = BucketListCRUD()
        
        # Filter out None values
        update_data = {k: v for k, v in item.dict().items() if v is not None}
        
        if not update_data:
            raise HTTPException(status_code=400, detail="No update data provided")
        
        result = crud_ops.update_item(category, item_id, update_data)
        
        if result["success"]:
            return {
                "success": True,
                "message": f"Successfully updated {category} item",
                "item": result["item"]
            }
        else:
            raise HTTPException(status_code=400, detail=result["error"])
            
    except Exception as e:
        logger.error(f"Error updating {category} item {item_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/categories/tv_shows/items/{show_id}/episodes")
async def get_tv_show_episodes(
    show_id: str = Path(...)
):
    """Get episodes for a specific TV show"""
    try:
        crud_ops = BucketListCRUD()
        
        # Get TV show details first
        tv_show_db_id = crud_ops.database_mapping.get("tv_shows")
        if not tv_show_db_id:
            raise HTTPException(status_code=404, detail="TV Shows database not found")
        
        # Get the TV show to find its name
        notion_service = NotionService()
        show_result = notion_service._make_request("GET", f"{notion_service.base_url}/pages/{show_id}")
        
        if not show_result["success"]:
            raise HTTPException(status_code=404, detail="TV Show not found")
        
        show_data = notion_service._simplify_page_data(show_result["data"])
        show_name = show_data["properties"].get("Title") or show_data["properties"].get("Name") or ""
        
        # Get episodes database
        episodes_db_id = crud_ops.database_mapping.get("episodes")
        if not episodes_db_id:
            return {"success": True, "episodes": [], "show_name": show_name}
        
        # Query episodes database for episodes related to this show
        # For now, we'll get all episodes and filter by show name match
        episodes_result = crud_ops.read_items("episodes", limit=100)
        
        if episodes_result["success"]:
            # Use the proper relationship field "סדרה (Relation)" to match episodes to shows
            related_episodes = []
            
            logger.info(f"Searching episodes for show: '{show_name}' using relation field")
            logger.info(f"Total episodes in database: {len(episodes_result['items'])}")
            
            for episode in episodes_result["items"]:
                episode_props = episode["properties"]
                episode_title = episode_props.get("Title") or episode_props.get("Name") or ""
                
                # Check ALL possible relation field variations that might exist in Notion
                relation_fields = [
                    episode_props.get("סדרה (Relation)"),
                    episode_props.get("סדרה"),
                    episode_props.get("Relation"), 
                    episode_props.get("TV Show"),
                    episode_props.get("Show"),
                    episode_props.get("Series")
                ]
                
                # Check if any relation field matches the show
                matches = False
                match_reason = ""
                
                for relation_field in relation_fields:
                    if relation_field:
                        # Handle different types of relation field values
                        relation_value = ""
                        
                        # If it's a list (Notion relations are returned as lists of objects with IDs)
                        if isinstance(relation_field, list) and len(relation_field) > 0:
                            # For relation fields, we need to resolve the IDs to actual titles
                            # Since we can't easily do cross-database lookups here, we'll try to match
                            # the relation IDs against the show ID we're looking for
                            for relation_item in relation_field:
                                if isinstance(relation_item, dict) and 'id' in relation_item:
                                    # Check if this relation ID matches our show ID
                                    if relation_item['id'] == show_id:
                                        matches = True
                                        match_reason = f"Relation ID match: episode linked to show ID '{show_id}'"
                                        break
                                # Also handle cases where the relation might have been resolved to a title
                                elif isinstance(relation_item, dict) and 'title' in relation_item:
                                    relation_value = relation_item['title']
                                elif isinstance(relation_item, dict) and 'plain_text' in relation_item:
                                    relation_value = relation_item['plain_text']
                                else:
                                    relation_value = str(relation_item)
                        # If it's a dict (another common Notion format)
                        elif isinstance(relation_field, dict):
                            if 'id' in relation_field and relation_field['id'] == show_id:
                                matches = True
                                match_reason = f"Relation ID match: episode linked to show ID '{show_id}'"
                                break
                            elif 'title' in relation_field:
                                relation_value = relation_field['title']
                            elif 'plain_text' in relation_field:
                                relation_value = relation_field['plain_text']
                            else:
                                relation_value = str(relation_field)
                        # If it's a simple string
                        else:
                            relation_value = str(relation_field)
                        
                        # If we found a direct ID match, we're done
                        if matches:
                            break
                        
                        # Otherwise, try title-based matching
                        if relation_value and relation_value.strip():
                            show_name_clean = show_name.strip().lower()
                            relation_clean = relation_value.strip().lower()
                            
                            if (show_name_clean == relation_clean or 
                                show_name_clean in relation_clean or 
                                relation_clean in show_name_clean):
                                matches = True
                                match_reason = f"Relation title match: '{relation_value}' matches show '{show_name}'"
                                break
                
                if matches:
                    related_episodes.append(episode)
                    logger.info(f"Episode matched: '{episode_title}' - {match_reason}")
                else:
                    # Log for debugging what relation fields we found
                    relation_debug = []
                    for i, field in enumerate(relation_fields):
                        if field:
                            relation_debug.append(f"Field{i}: {field}")
                    logger.debug(f"Episode not matched: '{episode_title}' - Relations found: {relation_debug}")
            
            logger.info(f"Found {len(related_episodes)} episodes for show '{show_name}' using relation field")
            
            return {
                "success": True,
                "show_name": show_name,
                "show_id": show_id,
                "episodes": related_episodes,
                "count": len(related_episodes)
            }
        
        return {"success": True, "episodes": [], "show_name": show_name}
        
    except Exception as e:
        logger.error(f"Error getting episodes for show {show_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/categories/around_world/items/{country_id}/cities")
async def get_country_cities(
    country_id: str = Path(...)
):
    """Get cities/sub-items for a specific country in around_world category"""
    try:
        crud_ops = BucketListCRUD()
        
        # Get country details first
        around_world_db_id = crud_ops.database_mapping.get("around_world")
        if not around_world_db_id:
            raise HTTPException(status_code=404, detail="Around World database not found")
        
        # Get the country to find its name
        notion_service = NotionService()
        country_result = notion_service._make_request("GET", f"{notion_service.base_url}/pages/{country_id}")
        
        if not country_result["success"]:
            raise HTTPException(status_code=404, detail="Country not found")
        
        country_data = notion_service._simplify_page_data(country_result["data"])
        country_name = country_data["properties"].get("Title") or country_data["properties"].get("Name") or ""
        
        # Get all around_world items and filter for cities belonging to this country
        all_items_result = crud_ops.read_items("around_world", limit=200)
        
        if all_items_result["success"]:
            # Filter items that belong to this country (sub-items/cities)
            # Since there are no explicit relationship fields, use content-based matching
            related_cities = []
            
            for item in all_items_result["items"]:
                item_props = item["properties"]
                
                # Skip the parent country item itself
                if item["id"] == country_id:
                    continue
                
                item_title = item_props.get("Title") or item_props.get("Name") or ""
                
                # Strategy 1: Direct country name matching in city title
                # e.g., "Berlin, Germany" contains "Germany"
                country_name_clean = country_name.replace("🇩🇪", "").replace("🇹🇭", "").replace("🇩🇰", "").strip()
                
                # Strategy 2: Look for country-specific patterns
                belongs_to_country = False
                
                if country_name_clean.lower() in item_title.lower():
                    belongs_to_country = True
                    logger.info(f"City matches by name: {item_title} contains {country_name_clean}")
                
                # Strategy 3: Country-specific city matching
                if "🇩🇪" in country_name or "germany" in country_name.lower():
                    german_cities = ["berlin", "ברלין", "munich", "hamburg", "cologne", "frankfurt"]
                    if any(city in item_title.lower() for city in german_cities):
                        belongs_to_country = True
                        logger.info(f"German city detected: {item_title}")
                
                elif "🇹🇭" in country_name or "thailand" in country_name.lower():
                    thai_cities = ["bangkok", "chiang mai", "phuket", "pattaya", "krabi", "samui", "koh samui"]
                    if any(city in item_title.lower() for city in thai_cities):
                        belongs_to_country = True
                        logger.info(f"Thai city detected: {item_title}")
                
                elif "🇩🇰" in country_name or "denmark" in country_name.lower():
                    danish_cities = ["copenhagen", "aarhus", "odense", "aalborg"]
                    if any(city in item_title.lower() for city in danish_cities):
                        belongs_to_country = True
                        logger.info(f"Danish city detected: {item_title}")
                
                # Strategy 4: Exclude items that are clearly other countries (have country flags)
                other_country_flags = ["🇺🇸", "🇫🇷", "🇮🇱", "🇬🇧", "🇮🇹", "🇪🇸", "🇳🇱", "🇨🇭", "🇦🇹", "🇯🇵", "🇰🇷"]
                # Remove the current country's flag from the exclusion list
                current_country_flags = []
                if "🇩🇪" in country_name:
                    current_country_flags = ["🇩🇪"]
                elif "🇹🇭" in country_name:
                    current_country_flags = ["🇹🇭"] 
                elif "🇩🇰" in country_name:
                    current_country_flags = ["🇩🇰"]
                
                exclusion_flags = [flag for flag in other_country_flags if flag not in current_country_flags]
                has_other_country_flag = any(flag in item_title for flag in exclusion_flags)
                
                if has_other_country_flag:
                    belongs_to_country = False
                    logger.info(f"Item excluded (other country flag): {item_title}")
                
                if belongs_to_country:
                    related_cities.append(item)
                    logger.info(f"City added to {country_name}: {item_title}")
            
            logger.info(f"Found {len(related_cities)} cities for {country_name}")
            
            return {
                "success": True,
                "country_name": country_name,
                "country_id": country_id,
                "cities": related_cities,
                "count": len(related_cities)
            }
        
        return {"success": True, "cities": [], "country_name": country_name}
        
    except Exception as e:
        logger.error(f"Error getting cities for country {country_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/categories/{category}/items/{item_id}", response_model=Dict[str, Any])
async def delete_bucket_list_item(
    category: str = Path(..., pattern="^(live_shows|dining_out|around_world|tv_shows|podcasts|books|movies|episodes)$"),
    item_id: str = Path(..., min_length=32, max_length=40)
):
    """Delete (archive) a bucket list item"""
    try:
        crud_ops = BucketListCRUD()
        result = crud_ops.delete_item(category, item_id)
        
        if result["success"]:
            return {
                "success": True,
                "message": f"Successfully deleted {category} item"
            }
        else:
            raise HTTPException(status_code=400, detail=result["error"])
            
    except Exception as e:
        logger.error(f"Error deleting {category} item {item_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/debug/test-database-creation")
async def test_database_creation():
    """Debug endpoint to test database creation"""
    try:
        db_creator = DatabaseCreator()
        
        # Test if parent page is accessible
        notion_service = NotionService()
        parent_test = notion_service._make_request("GET", f"{notion_service.base_url}/pages/{settings.parent_page_id}")
        
        if not parent_test["success"]:
            return {
                "error": "Parent page not accessible",
                "parent_page_id": settings.parent_page_id,
                "parent_test_result": parent_test
            }
        
        # Try to create books database
        config = db_creator.required_databases["books"]
        result = db_creator._create_new_database("books", config)
        
        return {
            "success": result["success"],
            "result": result,
            "parent_page_id": settings.parent_page_id
        }
        
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/categories", response_model=Dict[str, Any])
async def get_categories():
    """Get list of all available bucket list categories"""
    categories = {
        "live_shows": {
            "name": "Live Shows",
            "description": "Concerts, theater, comedy shows, and live performances",
            "icon": "🎭"
        },
        "dining_out": {
            "name": "Dining Out", 
            "description": "Restaurants, cafes, and culinary experiences",
            "icon": "🍽️"
        },
        "around_world": {
            "name": "Around the World",
            "description": "Travel destinations and global experiences", 
            "icon": "🌍"
        },
        "tv_shows": {
            "name": "TV Shows",
            "description": "Television series and streaming content",
            "icon": "📺"
        },
        "podcasts": {
            "name": "Podcasts",
            "description": "Podcast series and episodes",
            "icon": "🎧"
        },
        "books": {
            "name": "Books",
            "description": "Books to read and literary experiences",
            "icon": "📚"
        },
        "movies": {
            "name": "Movies", 
            "description": "Films and cinema experiences",
            "icon": "🎬"
        }
    }
    
    return {
        "success": True,
        "categories": categories,
        "count": len(categories)
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
