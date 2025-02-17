import React, { useState, useEffect, useContext, useMemo } from 'react';
import { AppContext } from '../../common/app-context';
import { LoadExcelClient } from '../../common/api-client/load-excel';
import {
  Box,
  Button,
  ColumnLayout,
  FormField,
  Header,
  Select,
  Table,
  Spinner,
  Alert,
  Checkbox,
  Flashbar,
} from '@cloudscape-design/components';
import '../../styles/resources.css';
import { useNavigate } from 'react-router-dom';
import { v4 as uuidv4 } from 'uuid';

const ResourcesPage: React.FC = () => {
  const appContext = useContext(AppContext);
  const loadExcelClient = useMemo(() => new LoadExcelClient(appContext), [appContext]);
  const navigate = useNavigate();

  const [data, setData] = useState<any[]>([]);
  const [dropdownOptions, setDropdownOptions] = useState<{ [key: string]: { label: string; value: string }[] }>({});
  const [dropdowns, setDropdowns] = useState<{ [key: string]: { label: string; value: string } | null }>({});
  const [filteredData, setFilteredData] = useState<any[]>([]);
  const [isLoading, setIsLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [checkboxSelections, setCheckboxSelections] = useState<{ [key: string]: Set<string> }>({});
  const [checkboxOptions, setCheckboxOptions] = useState<{ [key: string]: string[] }>({});
  const [hasFilteredWithSelections, setHasFilteredWithSelections] = useState(false);
  const [hasFiltered, setHasFiltered] = useState<boolean>(false);
  const [warningVisible, setWarningVisible] = useState<boolean>(false);
  const [hasSelections, setHasSelections] = useState(false);
  const [scrollBannerVisible, setScrollBannerVisible] = useState<boolean>(true);

  // Fetch data from the backend
  useEffect(() => {
    const fetchData = async () => {
      setIsLoading(true);
      setError(null);

      try {
        const jsonData = await loadExcelClient.loadExcelData();

        const validDropdowns = jsonData.dropdowns || {};
        const validRecords = jsonData.records || [];
        const validCheckboxes = jsonData.checkboxes || {};

        console.log('Fetched Checkboxes:', jsonData.checkboxes);

        // Transform dropdown options for Cloudscape Select
        const transformedDropdowns = Object.keys(validDropdowns).reduce((acc, key) => {
          acc[key] = [
            { label: "No selection", value: "" },  // Add empty option
            ...validDropdowns[key].map((option: string) => ({
              label: option.toString(),
              value: option.toString(),
            }))
          ];
          return acc;
        }, {});
        setDropdownOptions(transformedDropdowns);

        // Store the checkbox options
        setCheckboxOptions(validCheckboxes);

        // Initialize checkbox states
        const initialCheckboxSelections = Object.keys(validCheckboxes).reduce((acc, key) => {
          acc[key] = new Set(); // Initialize each group as an empty Set
          return acc;
        }, {});
        setCheckboxSelections(initialCheckboxSelections);

        // Initialize dropdown states
        const initialDropdowns = Object.keys(validDropdowns).reduce((acc, key) => {
          acc[key] = null;
          return acc;
        }, {});
        setDropdowns(initialDropdowns);

        // Set data
        setData(validRecords);
        setFilteredData(validRecords);
      } catch (err) {
        setError('Failed to load data.');
      } finally {
        setIsLoading(false);
      }
    };

    fetchData();
  }, [loadExcelClient]);

  // Handle dropdown changes
  const handleDropdownChange = (key: string, selectedOption: { label: string; value: string } | null) => {
    setDropdowns((prev) => ({
      ...prev,
      [key]: selectedOption,
    }));
    checkForSelections();
  };

  // Add selection check for checkboxes
  const handleCheckboxChange = (group: string, option: string, checked: boolean) => {
    setCheckboxSelections((prev) => {
      const updated = new Set(prev[group]);
      if (checked) {
        updated.add(option);
      } else {
        updated.delete(option);
      }
      return { ...prev, [group]: updated };
    });
    checkForSelections();
  };

  // Function to check if any selections have been made
  const checkForSelections = () => {
    const hasDropdownSelections = Object.values(dropdowns).some(value => 
      value !== null && value !== undefined && value.value !== ""
    );
    
    const hasCheckboxSelections = Object.values(checkboxSelections).some(
      selections => selections && selections.size > 0
    );

    setHasSelections(hasDropdownSelections || hasCheckboxSelections);
    setScrollBannerVisible(true);
  };

  const handleNavigateToAI = () => {
    const newSessionId = uuidv4();
    const resourcesList = filteredData.map(item => 
      `${item['Resource Name']}`
    ).join(', ');

    // Create two prompts: one for display and one for processing
    const displayPrompt = "Finding more information about the selected grants and programs...";
    const actualPrompt = `Based on the filters selected, I found these resources: ${resourcesList}. 
    Could you please summarize these resources and their key benefits,
    and highlight all eligibility requirements or deadlines (if any) for each resource? Maintain all of 
    the formatting requirements of the original system prompt.`;

    navigate(`/chatbot/playground/${newSessionId}`, { 
      state: { 
        prompt: actualPrompt 
      } 
    });
  };
    // Navigate with both prompts
  //   navigate(`/chatbot/playground/${newSessionId}`, { 
  //     state: { 
  //       displayPrompt,
  //       actualPrompt 
  //     } 
  //   });
  // };

  // Filter data based on dropdown and checkbox selections
  const filterData = () => {
    // Safely check dropdowns with null checks
    const hasDropdownSelections = dropdowns && Object.values(dropdowns).some(value => 
      value !== null && value !== undefined && value.value !== ""
    );
    
    // Safely check checkbox selections with null checks
    const hasCheckboxSelections = checkboxSelections && Object.values(checkboxSelections).some(
      selections => selections && selections.size > 0
    );

    // Check if any selections were made
    if (!hasDropdownSelections && !hasCheckboxSelections) {
      setWarningVisible(true);
      setHasFiltered(false); // Ensure we stay in unfiltered state
      return;
    }

    setWarningVisible(false);
    // Your existing filter logic
    let filtered = [...data];
    console.log('Starting filter with records:', filtered.length);

    // 1. Apply AND logic for dropdowns and Category
    // First, handle dropdowns (Size and Life Cycle)
    const activeDropdowns = Object.entries(dropdowns || {}).filter(([_, value]) => value !== null);
    if (activeDropdowns.length > 0) {
      filtered = filtered.filter(item => {
        return activeDropdowns.every(([_, value]) => {
          if (!value?.value) return true;
          return item[value.value] === 1;
        });
      });
    }

    // Then, handle Category checkboxes with AND logic
    const categorySelections = checkboxSelections['Category'] || new Set();
    if (categorySelections.size > 0) {
      filtered = filtered.filter(item => {
        return Array.from(categorySelections).some(selection => item[selection] === 1);
      });
    }

    // 2. Apply OR logic for all other checkbox groups
    const otherCheckboxGroups = Object.entries(checkboxSelections)
      .filter(([group, _]) => group !== 'Category')
      .filter(([_, selections]) => selections.size > 0);

    if (otherCheckboxGroups.length > 0) {
      const allOtherSelections = otherCheckboxGroups
        .map(([_, selections]) => Array.from(selections))
        .flat();

      if (allOtherSelections.length > 0) {
        filtered = filtered.filter(item => {
          // Item matches if it has ANY of the selected checkboxes from any group
          return allOtherSelections.some(selection => item[selection] === 1);
        });
      }
    }

    console.log('Final filtered count:', filtered.length);
    setFilteredData(filtered);
    setHasFiltered(true);
  };

  const handleDeleteRecord = (resourceName: string) => {
    setFilteredData((prevData) => prevData.filter(item => item['Resource Name'] !== resourceName));
  };

  if (isLoading) {
    return (
      <div style={{ textAlign: 'center', marginTop: '50px' }}>
        <Spinner size="large" />
        <p>Loading data...</p>
      </div>
    );
  }

  if (error) {
    return (
      <Alert type="error" header="Error loading data">
        {error}
      </Alert>
    );
  }

  if (!data.length || !Object.keys(dropdownOptions).length) {
    return <p>No data available to display.</p>;
  }

  return (
    <div style={{ margin: 0, padding: 0 }}>
      {/* Main Header */}
      <div style={{ 
        textAlign: 'center', 
        padding: '40px 0', 
        backgroundColor: '#001f3f',
        margin: 0,
        position: 'relative',
        width: '100%'
      }}>
        <div style={{ 
          fontSize: '48px', 
          fontWeight: 'bold', 
          color: '#d1e3f0', 
          textAlign: 'center' 
        }}>
          Filter Grants and Programs
        </div>
        <p style={{ 
          fontSize: '20px', 
          color: '#d1e3f0', 
          margin: '10px 0' 
        }}>
          Use the filters below to find relevant programs
        </p>
      </div>

      {/* Scroll Banner after header */}
      {hasSelections && !hasFiltered && scrollBannerVisible && (
        <div style={{ 
          position: 'fixed',
          top: 0,
          left: 0,
          right: 0,
          zIndex: 2000,
          width: '100%'
        }}>
          <Flashbar
            items={[
              {
                type: "info",
                content: "Scroll down to filter results once all selections are made",
                dismissible: true,
                dismissLabel: "Dismiss message",
                onDismiss: () => setScrollBannerVisible(false),
                id: "scroll-banner"
              }
            ]}
          />
        </div>
      )}

      {/* Main Content */}
      <Box padding={{ horizontal: 'xxxl', top: 'xxl' }}>
        {/* Primary Filters Section */}
        <div style={{
          backgroundColor: '#e8f0fa',
          padding: '30px',
          borderRadius: '15px',
          marginBottom: '40px',
          marginTop: '20px'
        }}>
          <Header
            variant="h1"
            description="Start by selecting these primary filters"
          >
            Step 1: Primary Filters
          </Header>
          {error && (
            <Alert
              type="warning"
              statusIconAriaLabel="Warning"
              header="No filters selected"
              dismissible
              onDismiss={() => setError(null)}
            >
              {error}
            </Alert>
          )}
          {/* Primary Dropdowns (Life Cycle and Size) */}
          <ColumnLayout columns={2} borders="vertical">
            {Object.entries(dropdownOptions)
              .filter(([key, _]) => ['Life Cycle', 'Size'].includes(key))
              .map(([key, options]) => (
                <FormField 
                  key={key} 
                  //label={`Select ${key}`}
                  description=" "  // Added empty description for extra spacing
                  stretch={true}
                >
                  <div style={{ fontSize: '18px', fontWeight: 'bold', marginBottom: '8px' }}>
                    {`Select ${key}`}
                  </div>
                  <Select
                    selectedOption={dropdowns[key] || { label: "No selection", value: "" }}
                    onChange={({ detail }) => {
                      const selectedOption = detail.selectedOption;
                      if (selectedOption.value === "") {
                        handleDropdownChange(key, null);
                      } else if (selectedOption) {
                        handleDropdownChange(key, {
                          label: selectedOption.label || "",
                          value: selectedOption.value
                        });
                      }
                    }}
                    options={options}
                    placeholder={`Select ${key}`}
                  />
                </FormField>
              ))}
          </ColumnLayout>

          {/* Category Checkboxes */}
          <Box margin={{ top: "l" }}>
            <Header variant="h2">
              Select Business Category
            </Header>
            <Box
              margin={{ top: "m" }}
            >
              {(checkboxOptions['Category'] || []).map((option) => (
                <Box padding="xs">
                  <Checkbox
                    key={`Category-${option}`}
                    checked={checkboxSelections['Category']?.has(option) || false}
                    onChange={({ detail }) => {
                      handleCheckboxChange('Category', option, detail.checked);
                    }}
                  >
                    <span style={{ fontSize: '16px' }}>{option}</span>
                  </Checkbox>
                </Box>
              ))}
            </Box>
          </Box>
        </div>

        {/* Secondary Filters Section */}
        <div style={{
          backgroundColor: '#f5f7fa',
          padding: '30px',
          borderRadius: '15px',
          marginBottom: '40px'
        }}>
          <Header
            variant="h1"
            description="Select additional filters to refine your search"
          >
            Step 2: Additional Filters
          </Header>

          {/* Other Checkbox Groups */}
          {Object.entries(checkboxOptions)
            .filter(([group, _]) => group !== 'Category')
            .map(([group, options]) => (
              <Box key={group} margin={{ bottom: 'l' }}>
                <Header variant="h3">{group}</Header>
                <div style={{ 
                  display: 'grid', 
                  gridTemplateColumns: 'repeat(auto-fill, minmax(250px, 1fr))',
                  gap: '15px',
                  fontSize: '16px'  // Increased font size
                }}>
                  {options.map((option) => (
                    <Checkbox
                      key={`${group}-${option}`}
                      checked={checkboxSelections[group]?.has(option) || false}
                      onChange={({ detail }) => {
                        handleCheckboxChange(group, option, detail.checked);
                      }}
                    >
                      {option}
                    </Checkbox>
                  ))}
                </div>
              </Box>
            ))}
        </div>

        {/* Warning Banner and Filter Results Button */}
        <Box textAlign="center" margin={{ bottom: 'xl' }}>
          {warningVisible && (
            <Box margin={{ bottom: 'm' }}>
              <Flashbar
                items={[
                  {
                    type: "warning",
                    content: "Please make at least one selection before filtering.",
                    dismissible: true,
                    dismissLabel: "Dismiss warning message",
                    onDismiss: () => setWarningVisible(false),
                    id: "filter-warning"
                  }
                ]}
              />
            </Box>
          )}
          
          <div style={{ display: 'flex', justifyContent: 'center', gap: '40px'}}>
            <Button 
              variant="primary" 
              onClick={filterData}
              className="filter_button"
            >
              Filter Results
            </Button>

            {hasFiltered && (
              <Button
                variant="normal"
                onClick={handleNavigateToAI}
                iconName="contact"
                className="ai_button"
              >
                Summarize Resources
              </Button>
            )}
          </div>
        </Box>

        {/* Results Section */}
        <Box margin={{ top: 'xl' }}>
          {error && (
            <Alert type="error" dismissible onDismiss={() => setError(null)}>
              {error}
            </Alert>
          )}
          
          <Table
            header={<Header variant="h2">{hasFiltered ? "Filtered Results" : "Unfiltered Results"}</Header>}
            columnDefinitions={[
              {
                id: 'Agency',
                header: 'Agency',
                cell: (item) => item['Agency'] || '-',
              },
              {
                id: 'Resource Name',
                header: 'Resource Name',
                cell: (item) => item['Resource Name'] || '-',
              },
              {
                id: 'Task Type',
                header: 'Task Type',
                cell: (item) => item['Task Type'] || '-',
              },
              ...(hasFiltered ? [{
                id: 'Actions',
                header: 'Actions',
                cell: (item) => (
                  <Button
                    variant="link"
                    onClick={() => handleDeleteRecord(item['Resource Name'])}
                  >
                    Remove
                  </Button>
                ),
              }] : []),
            ]}
            items={hasFiltered ? filteredData : data}
            wrapLines
            stripedRows
          />
        </Box>
      </Box>
    </div>
  );
};
export default ResourcesPage;
