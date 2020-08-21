#!/usr/bin/env bash
coverage errase
coverage run -m unittest redistructures_test.py
coverage html
