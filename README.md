<div align="center">
  <img src="docs/logo.png" alt="Persona Distiller Logo" width="200"/>

# Persona Distiller

[![Python](https://img.shields.io/badge/Python-3.12+-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688.svg?logo=fastapi)](https://fastapi.tiangolo.com/)
[![ChromaDB](https://img.shields.io/badge/ChromaDB-1.5.7+-orange.svg)](https://docs.trychroma.com/)
[![FAISS](https://img.shields.io/badge/FAISS-CPU-lightgrey.svg)](https://github.com/facebookresearch/faiss)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

*A local web app for distilling document collections into persona imitation skills.* <br>
*一个将文档集合提炼为人物角色模仿技能的本地 Web 应用。*

[English](#english) | [中文](#中文)

</div>

---

<h2 id="english">🇬🇧 English</h2>

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Architecture](#architecture)
- [Installation](#installation)
- [Usage](#usage)

## Overview
**Persona Distiller** is a robust, locally-hosted web application designed to ingest and analyze diverse document collections, distilling them into highly accurate persona imitation skills. It leverages advanced vector databases and machine learning techniques to capture the nuanced tone, knowledge, and style of any provided text source.

## Features
- **Document Processing**: Supports multiple formats including PDF (`pypdf`), DOCX (`python-docx`), and HTML (`beautifulsoup4`).
- **High-Performance Vector Search**: Utilizes `ChromaDB` and `FAISS` for lightning-fast, highly accurate similarity search and embedding storage.
- **Modern Web Interface**: Built with a fast, async backend using `FastAPI` and server-side rendered templates via `Jinja2`.
- **Local & Secure**: Runs entirely locally on your machine, ensuring data privacy and security.
- **Scalable Storage**: Employs `SQLAlchemy` for relational data management.

## Tech Stack
- **Language**: Python 3.12+
- **Backend Framework**: FastAPI, Uvicorn
- **Vector Databases**: ChromaDB, FAISS (CPU)
- **Database ORM**: SQLAlchemy
- **Document Parsers**: BeautifulSoup4, PyPDF, python-docx
- **Templating**: Jinja2

## Architecture
The system is built on a modular architecture:
1. **Ingestion Layer**: Parses uploaded documents (PDF, DOCX, HTML) and extracts text.
2. **Processing Layer**: Chunks text and generates vector embeddings.
3. **Storage Layer**: 
   - *Relational DB (SQLAlchemy)*: Manages metadata, sessions, and persona configurations.
   - *Vector DBs (ChromaDB / FAISS)*: Stores embeddings for semantic retrieval.
4. **API & Web Layer (FastAPI & Jinja2)**: Provides RESTful endpoints and a user-friendly web interface for interaction.

## Installation

### Prerequisites
- Python >= 3.12
- pip (Python package manager)

### Steps
1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/persona-distiller.git
   cd persona-distiller
   ```

2. **Create a virtual environment (optional but recommended):**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. **Install dependencies:**
   ```bash
   pip install -e .
   ```

4. **Install development dependencies (if testing/developing):**
   ```bash
   pip install -e ".[dev]"
   playwright install
   ```

## Usage
1. **Start the server:**
   ```bash
   uvicorn app.main:app --reload
   ```
   *(Note: Ensure you point uvicorn to your actual FastAPI app instance path)*

2. **Access the Web UI:**
   Open your browser and navigate to `http://localhost:8000`

3. **API Documentation:**
   Swagger UI is available at `http://localhost:8000/docs`

---

<h2 id="中文">🇨🇳 中文</h2>

## 目录
- [项目概览](#项目概览)
- [核心特性](#核心特性)
- [技术栈](#技术栈)
- [系统架构](#系统架构)
- [安装指南](#安装指南)
- [使用说明](#使用说明)

## 项目概览
**Persona Distiller** 是一个强大的本地 Web 应用程序，旨在摄取和分析多样化的文档集合，并将其提炼为高精度的人物角色模仿技能。它利用先进的向量数据库和机器学习技术，精准捕捉任何文本源的语调、知识和风格细节。

## 核心特性
- **多格式文档处理**：支持解析 PDF (`pypdf`)、DOCX (`python-docx`) 和 HTML (`beautifulsoup4`)。
- **高性能向量检索**：采用 `ChromaDB` 和 `FAISS` 实现极速、高准确度的相似性搜索和嵌入（Embedding）存储。
- **现代化 Web 接口**：基于 `FastAPI` 构建快速的异步后端，并使用 `Jinja2` 进行服务端模板渲染。
- **本地化与安全**：完全在本地机器上运行，确保数据的绝对隐私与安全。
- **可扩展存储**：使用 `SQLAlchemy` 进行关系型数据管理。

## 技术栈
- **编程语言**：Python 3.12+
- **后端框架**：FastAPI, Uvicorn
- **向量数据库**：ChromaDB, FAISS (CPU)
- **数据库 ORM**：SQLAlchemy
- **文档解析器**：BeautifulSoup4, PyPDF, python-docx
- **模板引擎**：Jinja2

## 系统架构
系统采用模块化架构设计：
1. **摄取层 (Ingestion)**：解析上传的文档（PDF、DOCX、HTML）并提取纯文本。
2. **处理层 (Processing)**：对文本进行分块处理，并生成向量嵌入。
3. **存储层 (Storage)**：
   - *关系型数据库 (SQLAlchemy)*：管理元数据、会话和角色配置信息。
   - *向量数据库 (ChromaDB / FAISS)*：存储嵌入向量以供语义检索。
4. **API & Web 层 (FastAPI & Jinja2)**：提供 RESTful 接口和用户友好的 Web 交互界面。

## 安装指南

### 环境要求
- Python >= 3.12
- pip (Python 包管理器)

### 安装步骤
1. **克隆仓库：**
   ```bash
   git clone https://github.com/yourusername/persona-distiller.git
   cd persona-distiller
   ```

2. **创建虚拟环境（可选但推荐）：**
   ```bash
   python -m venv venv
   source venv/bin/activate  # Windows 用户请使用 `venv\Scripts\activate`
   ```

3. **安装项目依赖：**
   ```bash
   pip install -e .
   ```

4. **安装开发依赖（用于测试/开发）：**
   ```bash
   pip install -e ".[dev]"
   playwright install
   ```

## 使用说明
1. **启动服务：**
   ```bash
   uvicorn app.main:app --reload
   ```
   *（注意：请确保 uvicorn 指向实际的 FastAPI app 实例路径）*

2. **访问 Web 界面：**
   打开浏览器并访问 `http://localhost:8000`

3. **API 文档：**
   可以在 `http://localhost:8000/docs` 查看自动生成的 Swagger UI 接口文档。
