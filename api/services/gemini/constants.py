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
