name: Config Update

on:
  push:
    branches: [ main ]
  workflow_dispatch:
  schedule:
    - cron: '0 12 * * *'

jobs:
  build:
    runs-on: ubuntu-latest

    steps:
    - uses: actions/checkout@v2
    - name: "Setup sing-box"
      env:
        SING_BOX_DEB_URL: "https://github.com/SagerNet/sing-box/releases/download/v1.8.0-rc.7/sing-box_1.8.0-rc.7_linux_amd64.deb"
      run: |
          set -Eeuo pipefail
          wget -O sing-box.deb $SING_BOX_DEB_URL
          sudo dpkg -i sing-box.deb
    - name: Set up Python3
      uses: actions/setup-python@v2
      with:
        python-version: 3.x
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install pandas requests pyyaml
    - name: Run script
      run: python ../main.py
      working-directory: ./rule/
    - name: Commit and push config.json
      run: |
        git config --global user.email "action@github.com"
        git config --global user.name "GitHub Action"
        git add ./rule/*.json
        git add ./rule/*.srs

        # 检查是否有文件被修改
        if git diff --staged --quiet; then
          echo "No changes to commit"
          exit 0
        else
          git commit -m "Update rules"
          git push
        fi
