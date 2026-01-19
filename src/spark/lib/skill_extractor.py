"""
Extract skills from job titles and descriptions.
"""
from typing import List, Set
import re

class SkillExtractor:
    # Skill taxonomy for Sri Lankan IT market
    SKILLS = {
        # Programming Languages
        'python': ['python', 'python3', 'py'],
        'java': ['java', 'j2ee', 'jdk'],
        'javascript': ['javascript', 'js', 'es6', 'ecmascript'],
        'typescript': ['typescript', 'ts'],
        'csharp': ['c#', 'csharp', '.net', 'dotnet'],
        'php': ['php', 'laravel', 'symfony'],
        'golang': ['golang', 'go lang'],
        'rust': ['rust', 'rustlang'],
        'scala': ['scala'],

        # Frontend
        'react': ['react', 'reactjs', 'react.js'],
        'angular': ['angular', 'angularjs'],
        'vue': ['vue', 'vuejs', 'vue.js'],
        'html': ['html', 'html5'],
        'css': ['css', 'css3', 'sass', 'scss'],

        # Backend
        'nodejs': ['nodejs', 'node.js', 'node'],
        'django': ['django'],
        'flask': ['flask'],
        'spring': ['spring', 'springboot', 'spring boot'],
        'fastapi': ['fastapi'],

        # Data
        'sql': ['sql', 'mysql', 'postgresql', 'postgres', 'mssql'],
        'nosql': ['mongodb', 'nosql', 'cassandra', 'dynamodb'],
        'spark': ['spark', 'pyspark', 'apache spark'],
        'hadoop': ['hadoop', 'hdfs', 'hive'],
        'kafka': ['kafka', 'apache kafka'],
        'airflow': ['airflow', 'apache airflow'],
        'etl': ['etl', 'data pipeline', 'data engineering'],

        # Cloud
        'aws': ['aws', 'amazon web services', 'ec2', 's3', 'lambda'],
        'azure': ['azure', 'microsoft azure'],
        'gcp': ['gcp', 'google cloud', 'bigquery'],

        # DevOps
        'docker': ['docker', 'containerization'],
        'kubernetes': ['kubernetes', 'k8s'],
        'jenkins': ['jenkins', 'ci/cd', 'cicd'],
        'terraform': ['terraform', 'iac'],
        'git': ['git', 'github', 'gitlab', 'bitbucket'],

        # Other
        'agile': ['agile', 'scrum', 'kanban'],
        'linux': ['linux', 'unix', 'ubuntu', 'centos'],
        'api': ['api', 'rest', 'restful', 'graphql'],
        'machine_learning': ['machine learning', 'ml', 'ai', 'deep learning'],
    }

    @classmethod
    def extract_skills(cls, text: str) -> List[str]:
        """Extract skills from text."""
        if not text:
            return []

        text_lower = text.lower()
        found_skills: Set[str] = set()

        for skill_name, keywords in cls.SKILLS.items():
            for keyword in keywords:
                # Word boundary matching
                pattern = r'\b' + re.escape(keyword) + r'\b'
                if re.search(pattern, text_lower):
                    found_skills.add(skill_name)
                    break

        return sorted(list(found_skills))

def extract_skills_from_title(title: str) -> List[str]:
    """UDF-friendly wrapper."""
    return SkillExtractor.extract_skills(title)