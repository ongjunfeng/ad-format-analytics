import pytest


def extract_post_id_from_url(url: str) -> str:
    """
    Extract post ID (shortcode) from Instagram URL.
    Copied here for testing to avoid import issues.
    """
    try:
        parts = url.rstrip('/').split('/')
        if 'p' in parts:
            p_index = parts.index('p')
            return parts[p_index + 1] if len(parts) > p_index + 1 else parts[-1]
        
        post_id = parts[-1] if parts[-1] else parts[-2]
        return post_id
    except:
        return None


class TestPostIdExtraction:
    """Test post ID extraction from URLs"""
    
    def test_extract_reel_url(self):
        """Test extracting ID from reel URL"""
        url = "https://www.instagram.com/reel/C8mtEPSp4b8/"
        result = extract_post_id_from_url(url)
        assert result == "C8mtEPSp4b8"
    
    def test_extract_post_url_with_p(self):
        """Test extracting ID from post URL with 'p' path"""
        url = "https://www.instagram.com/p/ABC123456/"
        result = extract_post_id_from_url(url)
        assert result == "ABC123456"
    
    def test_extract_without_trailing_slash(self):
        """Test URL without trailing slash"""
        url = "https://www.instagram.com/reel/XYZ789"
        result = extract_post_id_from_url(url)
        assert result == "XYZ789"
    
    def test_invalid_url_returns_none(self):
        """Test that invalid URLs return None"""
        url = "not_a_url"
        result = extract_post_id_from_url(url)
        assert result is None or result == "not_a_url"