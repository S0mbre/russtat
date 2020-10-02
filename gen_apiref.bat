@echo off
set doxy="c:\_PROG_\Doxygen\doxygen.exe"
set doxydir="doc\ref"
%doxy% "%doxydir%\doxyfile"
"%doxydir%\html\index.html"