import os
import time
import json
import random
from pathlib import Path
from google import genai

VIDEO_ANALYSIS_PROMPT = """
    Please give a detailed breakdown of the entire video, 
    include the relevant timestamps and output in JSON List format.
    Also please include what the individual speaker(s) were saying, the tone, any setting descriptions, key visual moments. 
    Finally, at the base structure, give a speaker description detailing their appearance, gender, style, and personality.
            
    Provide similar JSON structure in this format:
    {
        "video_analysis": {
            "speaker_description": {
            "appearance": "Young adult male, approximately 25-30 years old, brown hair, casual attire",
            "gender": "Male",
            "style": "Casual gaming setup with RGB lighting, wearing a black hoodie",
            "personality": "Energetic, enthusiastic, prone to excited outbursts during intense moments"
            },
            "timestamps": [
            {
                "time": "00:00-00:15",
                "dialogue": "Hey everyone, welcome back to my channel! Today we're diving into this crazy new level.",
                "tone": "Excited, welcoming",
                "setting_description": "Gaming room with multiple monitors, LED strip lighting in background",
                "key_visual_moments": "Player adjusts headset, gestures toward screen",
                "action_context": "Introduction and setup"
            },
            {
                "time": "00:16-00:45",
                "dialogue": "Oh no, oh no, this is not going well... Wait, I think I can make this jump!",
                "tone": "Anxious, then hopeful",
                "setting_description": "Same gaming setup, player leaning forward intensely",
                "key_visual_moments": "Close-up of concentrated facial expression, rapid hand movements",
                "action_context": "Challenging gameplay sequence"
            },
            {
                "time": "00:46-01:20",
                "dialogue": "YES! Did you see that? That was absolutely insane!",
                "tone": "Triumphant, elated",
                "setting_description": "Player throws hands up in celebration",
                "key_visual_moments": "Victory gesture, big smile, pointing at screen",
                "action_context": "Successful completion of difficult section"
            }
            ]
        }
    }
"""


# Get API key from environment
api_key = os.getenv("GEMINI_API_KEY")
if not api_key:
    raise ValueError("GEMINI_API_KEY environment variable is required")

client = genai.Client(api_key=api_key)


def extract_post_id(post_url: str) -> str:
    """Extract post ID from Instagram URL."""
    try:
        parts = post_url.rstrip('/').split('/')
        if 'p' in parts:
            p_index = parts.index('p')
            return parts[p_index + 1]
    except (IndexError, ValueError):
        pass
    return post_url.rstrip('/').split('/')[-1]


def get_videos_with_metadata(video_dir: str, labeled_data: list, limit: int = None, random_sample: bool = False) -> list:
    """
    Get video files and match them with performance data from labeled_scraped_data.json.

    Args:
        video_dir: Directory containing video files (e.g., 'ig_videos/viral')
        labeled_data: List of post data from labeled_scraped_data.json
        limit: Maximum number of videos to return
        random_sample: If True, randomly sample videos; if False, sort by views and take top N

    Returns:
        List of dicts with 'video_path' and 'metadata' keys
    """
    video_path = Path(video_dir)
    if not video_path.exists():
        print(f"Warning: {video_dir} does not exist")
        return []

    video_files = list(video_path.glob("*.mp4"))
    results = []

    for video_file in video_files:
        # Video filename is the post ID (e.g., C8mtEPSp4b8.mp4)
        post_id = video_file.stem

        # Find matching metadata in labeled data
        metadata = None
        for post in labeled_data:
            post_url = post.get("Post URL", "")
            if post_id in post_url:
                metadata = post
                break

        if metadata:
            results.append({
                'video_path': str(video_file),
                'post_id': post_id,
                'metadata': metadata
            })

    if random_sample:
        # Randomly sample videos
        if limit and len(results) > limit:
            results = random.sample(results, limit)
    else:
        # Sort by views (descending) to get top performers
        # Handle None values in Views field
        results.sort(key=lambda x: x['metadata'].get('Views') or 0, reverse=True)
        if limit:
            results = results[:limit]

    return results


def analyze_video(video_path: str, metadata: dict, post_id: str) -> dict:
    """
    Analyze a single video using Gemini API.

    Returns:
        Dictionary containing video analysis and performance context
    """
    print(f"Analyzing {post_id}...")

    try:
        # Upload video for analysis
        myfile = client.files.upload(file=video_path)
        while myfile.state == "PROCESSING":
            print(f"  Waiting for {post_id} to be processed...")
            time.sleep(5)
            myfile = client.files.get(name=myfile.name)
        print(f"  Video uploaded successfully: {myfile.state}")

        # Get video content analysis with proper configuration
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=[myfile, VIDEO_ANALYSIS_PROMPT],
            config={
                'max_output_tokens': 8192,  # Ensure enough tokens for long responses
            }
        )

        # Debug: Print response structure
        print(f"  Response received for {post_id}")

        # Check if response has valid content
        video_analysis = None
        if response:
            # Try to access text directly
            try:
                video_analysis = response.text
                if video_analysis:
                    print(f"  ✓ Video analysis generated ({len(video_analysis)} chars)")
                else:
                    print(f"  ✗ response.text is empty")
            except Exception as text_error:
                print(f"  ✗ Error accessing response.text: {text_error}")

            # If text is None/empty, inspect the response object
            if not video_analysis:
                print(f"  Debugging response structure:")
                response_dict = response.to_dict() if hasattr(response, 'to_dict') else str(response)
                print(f"  Response dict: {json.dumps(response_dict, indent=2) if isinstance(response_dict, dict) else response_dict}")

                # Check candidates
                if hasattr(response, 'candidates') and response.candidates:
                    for idx, candidate in enumerate(response.candidates):
                        print(f"  Candidate {idx}:")
                        print(f"    - finish_reason: {candidate.finish_reason if hasattr(candidate, 'finish_reason') else 'N/A'}")
                        if hasattr(candidate, 'safety_ratings'):
                            print(f"    - safety_ratings: {candidate.safety_ratings}")
                        if hasattr(candidate, 'content'):
                            print(f"    - content: {candidate.content}")
                else:
                    print(f"  No candidates in response")

                video_analysis = "Error: No valid content generated by API"
        else:
            print(f"  ✗ Response object is None")
            video_analysis = "Error: No response from API"

    except Exception as e:
        print(f"  ✗ ERROR during video analysis for {post_id}: {str(e)}")
        import traceback
        traceback.print_exc()
        video_analysis = f"Error during analysis: {str(e)}"

    # Build performance context with None-safe formatting
    views = metadata.get('Views') or 0
    likes = metadata.get('Likes') or 0
    comments = metadata.get('Comments') or 0
    duration = metadata.get('Video Duration') or 0
    engagement_rate = (likes / max(views, 1)) * 100 if views else 0

    performance_context = f"""
This video has the following performance metrics:
- Is Viral: {metadata.get('viral', False)}
- Views: {views:,.0f}
- Likes: {likes:,}
- Comments: {comments}
- Duration: {duration} seconds
- Caption: "{metadata.get('Captions', '')}"
- Engagement Rate: {engagement_rate:.2f}%
- Date Posted: {metadata.get('Date', '')}

Based on the video analysis above and these performance metrics, explain why this video {"went viral" if metadata.get('viral') else "did not go viral"}. What specific elements in the content, timing, format, or presentation contributed to its {"high" if metadata.get('viral') else "low"} engagement?

Focus on what happened in the video, the implied comedy/other elements that contributed to its {"virality" if metadata.get('viral') else "performance"} and why.

The transcript and scenes of the video is: {video_analysis}
"""

    # Get virality analysis
    virality_analysis = None
    try:
        virality_response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=[performance_context],
            config={
                'max_output_tokens': 8192,
            }
        )

        if virality_response:
            try:
                virality_analysis = virality_response.text
                if virality_analysis:
                    print(f"  ✓ Virality analysis generated ({len(virality_analysis)} chars)")
                else:
                    print(f"  ✗ virality_response.text is empty")
                    virality_analysis = "Error: No virality analysis generated"
            except Exception as text_error:
                print(f"  ✗ Error accessing virality_response.text: {text_error}")
                virality_analysis = f"Error accessing text: {text_error}"
        else:
            virality_analysis = "Error: No virality response from API"

    except Exception as e:
        print(f"  ✗ ERROR during virality analysis for {post_id}: {str(e)}")
        virality_analysis = f"Error during virality analysis: {str(e)}"

    print(f"  Completed analysis for {post_id}")

    return {
        'post_id': post_id,
        'video_path': video_path,
        'metadata': metadata,
        'video_analysis': video_analysis,
        'performance_context': performance_context,
        'virality_analysis': virality_analysis
    }


# Main execution
print("Loading labeled data...")
with open("./datasets/instagram/labeled_scraped_data.json", "r") as f:
    labeled_data = json.load(f)

print("Getting viral videos (random sampling)...")
viral_videos = get_videos_with_metadata("./datasets/instagram/viral", labeled_data, limit=5, random_sample=True)
print(f"Found {len(viral_videos)} viral videos")

print("Getting non-viral videos (random sampling)...")
non_viral_videos = get_videos_with_metadata("./datasets/instagram/non_viral", labeled_data, limit=20, random_sample=True)
print(f"Found {len(non_viral_videos)} non-viral videos")

# # # Analyze viral videos
# print("\n" + "="*60)
# print("ANALYZING VIRAL VIDEOS")
# print("="*60)
# viral_analyses = []
# for idx, video_data in enumerate(viral_videos, 1):
#     print(f"\n[{idx}/{len(viral_videos)}] Processing {video_data['post_id']}...")
#     analysis = analyze_video(
#         video_data['video_path'],
#         video_data['metadata'],
#         video_data['post_id']
#     )
#     viral_analyses.append(analysis)

# Analyze non-viral videos
print("\n" + "="*60)
print("ANALYZING NON-VIRAL VIDEOS")
print("="*60)
non_viral_analyses = []
for idx, video_data in enumerate(non_viral_videos, 1):
    print(f"\n[{idx}/{len(non_viral_videos)}] Processing {video_data['post_id']}...")
    analysis = analyze_video(
        video_data['video_path'],
        video_data['metadata'],
        video_data['post_id']
    )
    non_viral_analyses.append(analysis)

# Save analyses to JSON files
print("\n" + "="*60)
print("SAVING ANALYSES")
print("="*60)

# with open("./viral_video_analyses.json", "w") as f:
#     json.dump(viral_analyses, f, indent=2)
# print(f"Saved {len(viral_analyses)} viral analyses to viral_video_analyses.json")

with open("./non_viral_video_analyses.json", "w") as f:
    json.dump(non_viral_analyses, f, indent=2)
print(f"Saved {len(non_viral_analyses)} non-viral analyses to non_viral_video_analyses.json")

print("\n" + "="*60)
print("ANALYSIS COMPLETE")
print("="*60)
# print(f"Total videos analyzed: {len(viral_analyses) + len(non_viral_analyses)}")
# print(f"Viral: {len(viral_analyses)}")
print(f"Non-viral: {len(non_viral_analyses)}")
