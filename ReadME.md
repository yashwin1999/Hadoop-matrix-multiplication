README: Matrix Multiplication Using Hadoop MapReduce

Prerequisites

Before running the code, ensure you have the following setup:

1.	Hadoop installed (version 3.3.2 is recommended).
2.	Java Development Kit (JDK) installed (version 8 or later).
3.	The project has been compiled into a JAR file using Maven or a similar build tool.

Files Included

1.	Java Source Code: The source code for matrix multiplication is included in two folders "Normal" and "Optimized", use the Multiply.java in Normal folder for the first part of the assignment, and the Multiply.java in the Optimized folder for the 2nd part of the assignment
2.	Input files given in the assignment and the input files (Large Dataset) that were used by me are provided in the zip file.

Running the Code

1. Running the Code for sample inputs that were provided

To run the code with M and N matrix given in the assignment files, use the Multiply.java file from "Normal" folder in the zip file, as well as the following command, make sure that the input files "M-matrix-small.txt" and "N-matrix-small.txt" are present in the directory:

-----------------------------------------------------------------------------------------------------------------------------------------
~/hadoop-3.3.2/bin/hadoop jar target/*.jar Multiply M-matrix-small.txt N-matrix-small.txt intermediate output
-----------------------------------------------------------------------------------------------------------------------------------------

2. Running the Code for input Files of large data sets

To run the code with the matrix of Large Dataset for part-2 of the assignment, use the Multiply.java file from "Optimized" folder in the zip file, as well as the following command, make sure that the input files "twotone.txt" and "twotone-2.txt" are present in the directory:

-----------------------------------------------------------------------------------------------------------------------------------------
~/hadoop-3.3.2/bin/hadoop jar target/*.jar Multiply twotone.txt twotone-2.txt intermediate output
-----------------------------------------------------------------------------------------------------------------------------------------
