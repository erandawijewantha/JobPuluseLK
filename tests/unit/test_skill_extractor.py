import pytest
from src.spark.lib.skill_extractor import SkillExtractor

class TestSkillExtractor:
    def test_extract_python(self):
        skills = SkillExtractor.extract_skills("Python Developer needed")
        assert "python" in skills

    def test_extract_multiple_skills(self):
        skills = SkillExtractor.extract_skills("Full Stack Developer - React, Node.js, PostgreSQL")
        assert "react" in skills
        assert "nodejs" in skills
        assert "sql" in skills

    def test_extract_no_skills(self):
        skills = SkillExtractor.extract_skills("Office Assistant")
        assert len(skills) == 0

    def test_extract_case_insensitive(self):
        skills = SkillExtractor.extract_skills("JAVA DEVELOPER with AWS experience")
        assert "java" in skills
        assert "aws" in skills