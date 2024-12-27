/*

    whipple_cams_to_json - Stephen Fegan - 2024-12-26

    Writes the Whipple camera geometry from the ChiLA C++ header to JSON

    This file is part of "pyfzreader"

    "pyfzreader" is free software: you can redistribute it and/or modify it under the
    terms of the GNU General Public License version 2 or later, as published by
    the Free Software Foundation.

    "pyfzreader" is distributed in the hope that it will be useful, but WITHOUT ANY
    WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
    A PARTICULAR PURPOSE.  See the GNU General Public License for more details.

*/

// Compile: g++ -o whipple_cams_to_json whipple_cams_to_json.cpp

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <sstream>
#include "WhippleCams.h"

// Function to convert a vector to a JSON array string
template <typename T>
std::string arrayToJsonArray(const T* array, size_t npix) {
    std::ostringstream oss;
    oss << "[";
    for (size_t i = 0; i < npix; ++i) {
        oss << array[i];
        if (i < npix - 1) {
            oss << ", ";
        }
    }
    oss << "]";
    return oss.str();
}

// Function to process neighbors into a JSON array of arrays
std::string neighborsToJsonArray(const int neighbors[][NUM_NEIGHBORS], size_t numTubes) {
    std::ostringstream oss;
    oss << "[ ";
    for (size_t i = 0; i < numTubes; ++i) {
        oss << "[ ";
        bool first = true;
        for (int j = 0; neighbors[i][j] != -1; ++j) {
            if(neighbors[i][j] >= numTubes) {
                continue;
            }
            if (!first) {
                oss << ", ";
            }
            oss << neighbors[i][j];
            first = false;
        }
        oss << " ]";
        if (i < numTubes - 1) {
            oss << ", ";
        }
    }
    oss << " ]";
    return oss.str();
}

// Helper function to write a dataset to JSON
void writeDataSetToJson(std::ostringstream& json,
                        unsigned nadc,
                        const float* posX, const float* posY, const float* radius,
                        const int neighbors[][NUM_NEIGHBORS],
                        size_t npix, bool more = true) {
    // Start the inner dictionary for the dataset
    json << "  \"" << nadc << "\": {\n";

    // Add nadc and npix
    json << "    \"nadc\": " << nadc << ",\n";
    json << "    \"npix\": " << npix << ",\n";

    // Add X
    json << "    \"x\": " << arrayToJsonArray(posX, npix) << ",\n";

    // Add Y
    json << "    \"y\": " << arrayToJsonArray(posY, npix) << ",\n";

    // Add R
    json << "    \"r\": " << arrayToJsonArray(radius, npix) << ",\n";

    // Add Neighbors
    json << "    \"neighbors\": " << neighborsToJsonArray(neighbors, npix) << "\n";

    // Close the inner dictionary
    if(more) {
        json << "  },\n";
    } else {
        json << "  }\n";
    }
}

// Main function
int main() {
    // Open a file to write the JSON output
    std::ofstream outputFile("whipple_cams.json");
    if (!outputFile) {
        std::cerr << "Error: Unable to open output file for writing." << std::endl;
        return 1;
    }

    // Start the JSON object
    std::ostringstream json;
    json << "{\n";

    json << "  \"header\": \"whipple_cams.json - Stephen Fegan - 2024-12-26\\n\\nPixel positions, radii and neighbor map for Whipple cameras, extracted from\\nWhippleCams.h, a part of ChiLA. The cameras are keyed by the number of ADC\\nchannels read out, which is larger than the number of pixels, each ADC having\\ntwelve channels.\\n\\nThis file is part of \\\"pyfzreader\\\".\\n\\n\\\"pyfzreader\\\" is free software: you can redistribute it and/or modify it under\\nthe terms of the GNU General Public License version 2 or later, as published by\\nthe Free Software Foundation.\\n\\n\\\"pyfzreader\\\" is distributed in the hope that it will be useful, but WITHOUT ANY\\nWARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR\\nA PARTICULAR PURPOSE.  See the GNU General Public License for more details.\\n\",\n";

    writeDataSetToJson(json, 120, WC109Xcoord, WC109Ycoord, WC109Radius, 
        WC109Neighbors, 109);
    writeDataSetToJson(json, 156, WC151Xcoord, WC151Ycoord, WC151Radius, 
        WC151Neighbors, 151);
    writeDataSetToJson(json, 336, WC331Xcoord, WC331Ycoord, WC331Radius, 
        WC331Neighbors, 331);
    writeDataSetToJson(json, 492, WC490Xcoord, WC490Ycoord, WC490Radius, 
        WC490Neighbors, 490);
    writeDataSetToJson(json, 384, WC490Xcoord, WC490Ycoord, WC490Radius, 
        WC490Neighbors, 379, false);

    // End the JSON object
    json << "}\n";

    // Write to file
    outputFile << json.str();

    // Close the file
    outputFile.close();

    return 0;
}
