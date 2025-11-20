"""
Image processing utilities for AI classification and EXIF extraction.
Uses CLIP model for landscape/nature detection and Pillow for EXIF metadata.
"""
import torch
from PIL import Image
from PIL.ExifTags import TAGS
from transformers import CLIPProcessor, CLIPModel
from datetime import datetime
from typing import Dict, Optional
import os


# Global model cache
_clip_model = None
_clip_processor = None


def load_clip_model():
    """
    Load CLIP model for image classification.
    Uses lightweight CLIP model for real-time classification.
    
    Requirements: 5.5
    """
    global _clip_model, _clip_processor
    
    if _clip_model is None:
        print("Loading CLIP model for image classification...")
        model_name = "openai/clip-vit-base-patch32"
        _clip_model = CLIPModel.from_pretrained(model_name)
        _clip_processor = CLIPProcessor.from_pretrained(model_name)
        print("CLIP model loaded successfully")
    
    return _clip_model, _clip_processor


async def classify_image(image_path: str) -> Dict:
    """
    Classify image content using CLIP model.
    Detects landscape/lake/nature content and rejects inappropriate content.
    
    Requirements: 5.1, 5.2
    
    Args:
        image_path: Path to the image file
        
    Returns:
        Dictionary with classification results:
        - is_approved: Boolean indicating if image is approved
        - rejection_reason: String explaining rejection (if not approved)
        - scores: Dictionary of confidence scores for each category
        - predicted_category: String with the highest scoring category
    """
    try:
        # Load model
        model, processor = load_clip_model()
        
        # Open and preprocess image
        image = Image.open(image_path).convert("RGB")
        
        # Define categories for classification
        # Approved categories: landscape, lake, nature scenes
        # Rejected categories: selfies, documents, food, indoor scenes
        categories = [
            "a photo of a beautiful lake landscape",
            "a photo of nature scenery with water",
            "a photo of outdoor landscape",
            "a photo of a person's face or selfie",
            "a photo of a document or text",
            "a photo of food or meal",
            "a photo of indoor scene or room"
        ]
        
        # Process inputs
        inputs = processor(
            text=categories,
            images=image,
            return_tensors="pt",
            padding=True
        )
        
        # Get predictions
        with torch.no_grad():
            outputs = model(**inputs)
            logits_per_image = outputs.logits_per_image
            probs = logits_per_image.softmax(dim=1)
        
        # Extract scores
        scores = {
            "landscape": float(probs[0][0]),
            "nature_water": float(probs[0][1]),
            "outdoor": float(probs[0][2]),
            "selfie": float(probs[0][3]),
            "document": float(probs[0][4]),
            "food": float(probs[0][5]),
            "indoor": float(probs[0][6])
        }
        
        # Determine approval based on scores
        # Approved if landscape/nature scores are high
        approved_score = max(scores["landscape"], scores["nature_water"], scores["outdoor"])
        rejected_score = max(scores["selfie"], scores["document"], scores["food"], scores["indoor"])
        
        # Threshold: approved score should be significantly higher than rejected score
        is_approved = approved_score > rejected_score and approved_score > 0.3
        
        # Determine rejection reason if not approved
        rejection_reason = None
        if not is_approved:
            if scores["selfie"] > 0.3:
                rejection_reason = "셀카나 인물 사진은 업로드할 수 없습니다"
            elif scores["document"] > 0.3:
                rejection_reason = "문서나 텍스트 이미지는 업로드할 수 없습니다"
            elif scores["food"] > 0.3:
                rejection_reason = "음식 사진은 업로드할 수 없습니다"
            elif scores["indoor"] > 0.3:
                rejection_reason = "실내 사진은 업로드할 수 없습니다"
            else:
                rejection_reason = "인경호 풍경 사진이 아닙니다"
        
        # Determine predicted category
        predicted_category = max(scores, key=scores.get)
        
        return {
            "is_approved": is_approved,
            "rejection_reason": rejection_reason,
            "scores": scores,
            "predicted_category": predicted_category,
            "confidence": float(max(probs[0]))
        }
        
    except Exception as e:
        print(f"Image classification error: {str(e)}")
        # Default to rejection on error
        return {
            "is_approved": False,
            "rejection_reason": "이미지 분석 중 오류가 발생했습니다",
            "scores": {},
            "predicted_category": "error",
            "confidence": 0.0,
            "error": str(e)
        }


def extract_exif_metadata(image_path: str) -> Dict:
    """
    Extract EXIF metadata from image file.
    Focuses on timestamp information for data reliability.
    
    Requirements: 5.3
    
    Args:
        image_path: Path to the image file
        
    Returns:
        Dictionary with EXIF metadata:
        - timestamp: datetime object of when photo was taken (if available)
        - camera_make: Camera manufacturer
        - camera_model: Camera model
        - gps_info: GPS coordinates (if available)
        - other metadata fields
    """
    try:
        # Open image
        image = Image.open(image_path)
        
        # Extract EXIF data
        exif_data = image.getexif()
        
        if not exif_data:
            return {
                "timestamp": None,
                "has_exif": False,
                "message": "No EXIF data found"
            }
        
        # Parse EXIF tags
        metadata = {
            "has_exif": True,
            "timestamp": None,
            "camera_make": None,
            "camera_model": None,
            "gps_info": None
        }
        
        for tag_id, value in exif_data.items():
            tag_name = TAGS.get(tag_id, tag_id)
            
            # Extract timestamp
            if tag_name == "DateTime" or tag_name == "DateTimeOriginal":
                try:
                    # EXIF datetime format: "YYYY:MM:DD HH:MM:SS"
                    timestamp = datetime.strptime(str(value), "%Y:%m:%d %H:%M:%S")
                    metadata["timestamp"] = timestamp
                except (ValueError, TypeError):
                    pass
            
            # Extract camera info
            elif tag_name == "Make":
                metadata["camera_make"] = str(value)
            elif tag_name == "Model":
                metadata["camera_model"] = str(value)
            
            # Extract GPS info
            elif tag_name == "GPSInfo":
                metadata["gps_info"] = value
        
        return metadata
        
    except Exception as e:
        print(f"EXIF extraction error: {str(e)}")
        return {
            "timestamp": None,
            "has_exif": False,
            "error": str(e)
        }
