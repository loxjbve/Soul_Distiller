#!/usr/bin/env python
import os
import requests
import json

def create_pr():
    token = os.environ.get('GITHUB_TOKEN')
    if not token:
        print("GITHUB_TOKEN not found")
        return
        
    headers = {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github.v3+json'
    }
    
    # Generate unique branch name if needed or use current branch
    branch_name = os.popen('git rev-parse --abbrev-ref HEAD').read().strip()
    
    data = {
        'title': 'perf: fix severe frontend and backend performance bottlenecks',
        'body': 'This PR fixes major performance bottlenecks causing frontend stuttering and slow page transitions:\n\n1. **DB Eager Loading**: Removed unconditional eager loading of massive JSON blobs (`facets`, `events`) in `_project_context` page loads.\n2. **WebSocket Load**: Optimized WebSocket to only fetch lightweight status for active tasks instead of entire DB document arrays every second.\n3. **Frontend DOM Thrashing**: Optimized `project_detail.html` to update DOM nodes in-place instead of replacing `innerHTML` of the entire list.\n4. **LLM Delta DB Spam**: Stopped storing `llm_delta` streaming events in DB to prevent DB bloat and heavy deserialization overhead.',
        'head': branch_name,
        'base': 'main'
    }
    
    repo_url = 'https://api.github.com/repos/loxjbve/--/pulls'
    
    response = requests.post(repo_url, headers=headers, data=json.dumps(data))
    if response.status_code == 201:
        print(f"Successfully created PR: {response.json()['html_url']}")
    else:
        print(f"Failed to create PR: {response.status_code} - {response.text}")

if __name__ == '__main__':
    create_pr()
