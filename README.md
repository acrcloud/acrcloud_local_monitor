{\rtf1\ansi\ansicpg936\cocoartf1404\cocoasubrtf460
{\fonttbl\f0\fnil\fcharset204 PTSans-Regular;\f1\fnil\fcharset134 .PingFangSC-Regular;}
{\colortbl;\red255\green255\blue255;\red0\green0\blue0;\red0\green0\blue233;}
{\*\listtable{\list\listtemplateid1\listhybrid{\listlevel\levelnfc23\levelnfcn23\leveljc0\leveljcn0\levelfollow0\levelstartat1\levelspace360\levelindent0{\*\levelmarker \{disc\}}{\leveltext\leveltemplateid1\'01\uc0\u8226 ;}{\levelnumbers;}\fi-360\li720\lin720 }{\listname ;}\listid1}
{\list\listtemplateid2\listhybrid{\listlevel\levelnfc0\levelnfcn0\leveljc0\leveljcn0\levelfollow0\levelstartat1\levelspace360\levelindent0{\*\levelmarker \{decimal\}.}{\leveltext\leveltemplateid101\'02\'00.;}{\levelnumbers\'01;}\fi-360\li720\lin720 }{\listname ;}\listid2}
{\list\listtemplateid3\listhybrid{\listlevel\levelnfc0\levelnfcn0\leveljc0\leveljcn0\levelfollow0\levelstartat1\levelspace360\levelindent0{\*\levelmarker \{decimal\}.}{\leveltext\leveltemplateid201\'02\'00.;}{\levelnumbers\'01;}\fi-360\li720\lin720 }{\listname ;}\listid3}}
{\*\listoverridetable{\listoverride\listid1\listoverridecount0\ls1}{\listoverride\listid2\listoverridecount0\ls2}{\listoverride\listid3\listoverridecount0\ls3}}
\deftab720
\pard\pardeftab720\sl360\sa257\partightenfactor0

\f0\b\fs38 \cf2 \expnd0\expndtw0\kerning0
\outl0\strokewidth0 \strokec2 ACRCloud Local Monitoring Service\
Overview\
\pard\pardeftab720\sl360\sa240\partightenfactor0

\f1\b0\fs24 \cf2 Local Monitoring Services is used to monitor live radio streams in your local server.\
\pard\pardeftab720\sl360\sa257\partightenfactor0

\f0\b\fs38 \cf2 Requirements\
\pard\tx220\tx720\pardeftab720\li720\fi-720\sl360\partightenfactor0
\ls1\ilvl0
\f1\b0\fs24 \cf2 \kerning1\expnd0\expndtw0 \outl0\strokewidth0 {\listtext	\'a1\'a4	}\expnd0\expndtw0\kerning0
\outl0\strokewidth0 \strokec2 Python 2.7\
\ls1\ilvl0\kerning1\expnd0\expndtw0 \outl0\strokewidth0 {\listtext	\'a1\'a4	}\expnd0\expndtw0\kerning0
\outl0\strokewidth0 \strokec2 Works on Linux\
\pard\pardeftab720\sl360\sa257\partightenfactor0

\f0\b\fs38 \cf2 How To Use\
\pard\tx220\tx720\pardeftab720\li720\fi-720\sl360\partightenfactor0
\ls2\ilvl0
\f1\b0\fs24 \cf2 \kerning1\expnd0\expndtw0 \outl0\strokewidth0 {\listtext	1.	}\expnd0\expndtw0\kerning0
\outl0\strokewidth0 \strokec2 You should register an account on the {\field{\*\fldinst{HYPERLINK "https://console.acrcloud.com/"}}{\fldrslt \cf3 \ul \ulc3 \strokec3 ACRCloud platform}}, and create an project with local type in Broadcast Monitoring, you will get access_key and access_secret, then add your live radio streams in your project.\
\ls2\ilvl0\kerning1\expnd0\expndtw0 \outl0\strokewidth0 {\listtext	2.	}\expnd0\expndtw0\kerning0
\outl0\strokewidth0 \strokec2 Clone the code in your local server.\
\ls2\ilvl0\kerning1\expnd0\expndtw0 \outl0\strokewidth0 {\listtext	3.	}\expnd0\expndtw0\kerning0
\outl0\strokewidth0 \strokec2 Modify configuration file (acrcloud_local_monitor/acrcloud_config.py), fill in your database configuration.\
\ls2\ilvl0\kerning1\expnd0\expndtw0 \outl0\strokewidth0 {\listtext	4.	}\expnd0\expndtw0\kerning0
\outl0\strokewidth0 \strokec2 Run python acrcloud_server.py\
\pard\pardeftab720\sl360\sa278\partightenfactor0

\f0\b\fs34 \cf2 Python Dependency Library\
\pard\tx220\tx720\pardeftab720\li720\fi-720\sl360\partightenfactor0
\ls3\ilvl0
\f1\b0\fs24 \cf3 \kerning1\expnd0\expndtw0 \outl0\strokewidth0 {\listtext	1.	}{\field{\*\fldinst{HYPERLINK "https://github.com/twisted/twisted"}}{\fldrslt \expnd0\expndtw0\kerning0
\ul \outl0\strokewidth0 \strokec3 Twisted}}\cf2 \expnd0\expndtw0\kerning0
\outl0\strokewidth0 \strokec2 \
\ls3\ilvl0\cf3 \kerning1\expnd0\expndtw0 \outl0\strokewidth0 {\listtext	2.	}{\field{\*\fldinst{HYPERLINK "https://github.com/seatgeek/fuzzywuzzy"}}{\fldrslt \expnd0\expndtw0\kerning0
\ul \outl0\strokewidth0 \strokec3 fuzzywuzzy}}\cf2 \expnd0\expndtw0\kerning0
\outl0\strokewidth0 \strokec2 \
\ls3\ilvl0\cf3 \kerning1\expnd0\expndtw0 \outl0\strokewidth0 {\listtext	3.	}{\field{\*\fldinst{HYPERLINK "https://pypi.python.org/pypi/beautifulsoup4"}}{\fldrslt \expnd0\expndtw0\kerning0
\ul \outl0\strokewidth0 \strokec3 beautifulsoup4}}\cf2 \expnd0\expndtw0\kerning0
\outl0\strokewidth0 \strokec2 \
\pard\tx220\tx720\pardeftab720\li720\fi-720\sl360\partightenfactor0
\ls3\ilvl0\cf2 \kerning1\expnd0\expndtw0 \outl0\strokewidth0 {\listtext	4.	}\expnd0\expndtw0\kerning0
\outl0\strokewidth0 \strokec2 ...\
}